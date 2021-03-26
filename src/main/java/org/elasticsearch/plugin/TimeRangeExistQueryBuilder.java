package org.elasticsearch.plugin;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;

/** Constructs a query that only match on documents that the field has a value in them. */
public class TimeRangeExistQueryBuilder extends AbstractQueryBuilder<TimeRangeExistQueryBuilder> {
  public static final String NAME = "timeExists";

  public static final ParseField FIELD_FIELD = new ParseField("field");
  private static final ParseField VALUE_FIELD = new ParseField("value");

  private final String fieldName;

  public TimeRangeExistQueryBuilder(String fieldName) {
    if (Strings.isEmpty(fieldName)) {
      throw new IllegalArgumentException("field name is null or empty");
    }
    this.fieldName = fieldName;
  }

  /** Read from a stream. */
  public TimeRangeExistQueryBuilder(StreamInput in) throws IOException {
    super(in);
    fieldName = in.readString();
  }

  public static TimeRangeExistQueryBuilder fromXContent(XContentParser parser) throws IOException {
    String fieldPattern = null;
    String queryName = null;
    float boost = AbstractQueryBuilder.DEFAULT_BOOST;

    XContentParser.Token token;
    String currentFieldName = null;
    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
      if (token == XContentParser.Token.FIELD_NAME) {
        currentFieldName = parser.currentName();
      } else if (token.isValue()) {
        if (FIELD_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
          fieldPattern = parser.text();
        } else if (AbstractQueryBuilder.NAME_FIELD.match(
            currentFieldName, parser.getDeprecationHandler())) {
          queryName = parser.text();
        } else if (AbstractQueryBuilder.BOOST_FIELD.match(
            currentFieldName, parser.getDeprecationHandler())) {
          boost = parser.floatValue();
        } else {
          throw new ParsingException(
              parser.getTokenLocation(),
              "["
                  + TimeRangeExistQueryBuilder.NAME
                  + "] query does not support ["
                  + currentFieldName
                  + "]");
        }
      } else {
        throw new ParsingException(
            parser.getTokenLocation(),
            "["
                + TimeRangeExistQueryBuilder.NAME
                + "] unknown token ["
                + token
                + "] after ["
                + currentFieldName
                + "]");
      }
    }

    if (fieldPattern == null) {
      throw new ParsingException(
          parser.getTokenLocation(),
          "[" + TimeRangeExistQueryBuilder.NAME + "] must be provided with a [field]");
    }

    TimeRangeExistQueryBuilder builder = new TimeRangeExistQueryBuilder(fieldPattern);
    builder.queryName(queryName);
    builder.boost(boost);
    return builder;
  }

  public static Query newFilter(
      QueryShardContext context, String fieldPattern, boolean checkRewrite) {

    Collection<String> fields = getMappedField(context, fieldPattern);

    if (fields.isEmpty()) {
      if (checkRewrite) {
        throw new IllegalStateException("Rewrite first");
      } else {
        return new MatchNoDocsQuery("unmapped field:" + fieldPattern);
      }
    }

    if (context.indexVersionCreated().before(Version.V_6_1_0)) {
      return newLegacyExistsQuery(context, fields);
    }

    if (fields.size() == 1) {
      String field = fields.iterator().next();
      return newFieldExistsQuery(context, field);
    }

    BooleanQuery.Builder boolFilterBuilder = new BooleanQuery.Builder();
    for (String field : fields) {
      boolFilterBuilder.add(newFieldExistsQuery(context, field), BooleanClause.Occur.SHOULD);
    }
    return new ConstantScoreQuery(boolFilterBuilder.build());
  }

  private static Query newLegacyExistsQuery(QueryShardContext context, Collection<String> fields) {
    // We create TermsQuery directly here rather than using FieldNamesFieldType.termsQuery()
    // so we don't end up with deprecation warnings
    if (fields.size() == 1) {
      Query filter = newLegacyExistsQuery(context, fields.iterator().next());
      return new ConstantScoreQuery(filter);
    }

    BooleanQuery.Builder boolFilterBuilder = new BooleanQuery.Builder();
    for (String field : fields) {
      Query filter = newLegacyExistsQuery(context, field);
      boolFilterBuilder.add(filter, BooleanClause.Occur.SHOULD);
    }
    return new ConstantScoreQuery(boolFilterBuilder.build());
  }

  private static Query newLegacyExistsQuery(QueryShardContext context, String field) {
    MappedFieldType fieldType = context.fieldMapper(field);
    String fieldName = fieldType != null ? fieldType.name() : field;
    return new TermQuery(new Term(FieldNamesFieldMapper.NAME, fieldName));
  }

  private static Query newFieldExistsQuery(QueryShardContext context, String field) {
    MappedFieldType fieldType = context.getMapperService().fieldType(field);
    if (fieldType == null) {
      // The field does not exist as a leaf but could be an object so
      // check for an object mapper
      if (context.getObjectMapper(field) != null) {
        return newObjectFieldExistsQuery(context, field);
      }
      return Queries.newMatchNoDocsQuery("User requested \"match_none\" query.");
    }
    Query filter = fieldType.existsQuery(context);
    return new ConstantScoreQuery(filter);
  }

  private static Query newObjectFieldExistsQuery(QueryShardContext context, String objField) {
    BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
    Collection<String> fields = context.simpleMatchToIndexNames(objField + ".*");
    for (String field : fields) {
      Query existsQuery = context.getMapperService().fieldType(field).existsQuery(context);
      booleanQuery.add(existsQuery, Occur.SHOULD);
    }
    return new ConstantScoreQuery(booleanQuery.build());
  }

  /**
   * Helper method to get field mapped to this fieldPattern
   *
   * @return return collection of fields if exists else return empty.
   */
  private static Collection<String> getMappedField(QueryShardContext context, String fieldPattern) {
    final FieldNamesFieldMapper.FieldNamesFieldType fieldNamesFieldType =
        (FieldNamesFieldMapper.FieldNamesFieldType)
            context.getMapperService().fieldType(FieldNamesFieldMapper.NAME);

    if (fieldNamesFieldType == null) {
      // can only happen when no types exist, so no docs exist either
      return Collections.emptySet();
    }

    final Collection<String> fields;
    if (context.getObjectMapper(fieldPattern) != null) {
      // the _field_names field also indexes objects, so we don't have to
      // do any more work to support exists queries on whole objects
      fields = Collections.singleton(fieldPattern);
    } else {
      fields = context.simpleMatchToIndexNames(fieldPattern);
    }

    if (fields.size() == 1) {
      String field = fields.iterator().next();
      MappedFieldType fieldType = context.getMapperService().fieldType(field);
      if (fieldType == null) {
        // The field does not exist as a leaf but could be an object so
        // check for an object mapper
        if (context.getObjectMapper(field) == null) {
          return Collections.emptySet();
        }
      }
    }

    return fields;
  }

  @Override
  protected void doWriteTo(StreamOutput out) throws IOException {
    out.writeString(fieldName);
  }

  /** @return the field name that has to exist for this query to match */
  public String fieldName() {
    return this.fieldName;
  }

  @Override
  protected void doXContent(XContentBuilder builder, Params params) throws IOException {
    builder.startObject(NAME);
    builder.field(FIELD_FIELD.getPreferredName(), fieldName);
    printBoostAndQueryName(builder);
    builder.endObject();
  }

  @Override
  protected Query doToQuery(QueryShardContext context) throws IOException {
    return newFilter(context, fieldName, true);
  }

  @Override
  protected int doHashCode() {
    return Objects.hash(fieldName);
  }

  @Override
  protected boolean doEquals(TimeRangeExistQueryBuilder other) {
    return Objects.equals(fieldName, other.fieldName);
  }

  @Override
  public String getWriteableName() {
    return NAME;
  }
}