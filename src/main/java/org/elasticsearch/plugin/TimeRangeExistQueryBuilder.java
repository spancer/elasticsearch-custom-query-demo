package org.elasticsearch.plugin;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;

/** Constructs a query that only match on documents that the field has a value in them. */
public class TimeRangeExistQueryBuilder extends AbstractQueryBuilder<TimeRangeExistQueryBuilder> {
  public static final String NAME = "timeRangeExist";

  public static final ParseField FIELDS_FIELD = new ParseField("fields");
  public static final ParseField TARGET_FIELD = new ParseField("target_field");
  private static final ParseField TARGET_VALUE = new ParseField("target_value");
  private static final ParseField MIN_MATCH_FIELD = new ParseField("minMatch");
  private static final ParseField TIME_INTERVAL_FIELD = new ParseField("timeInterval");

  // filed names, using comma to seperate.
  private final Map<String, Float> fieldsBoosts;
  private final String targetField;
  private final String targetValue;
  private final Integer minMatch;
  private final Long timeInterval;

  public TimeRangeExistQueryBuilder(
      String targetField, String targetValue, Integer minMatch, Long timeInterval, String... fields) {
    if (targetField==null || fields == null || targetValue == null || minMatch == null || timeInterval == null)
      throw new IllegalArgumentException(
          "fieldsName or targetFiled or targetValue or minMatch or timeInterval cannot be null.");
    this.targetField = targetField;
    this.targetValue = targetValue;
    this.minMatch = minMatch;
    this.timeInterval = timeInterval;
    this.fieldsBoosts = new TreeMap<>();
    for (String field : fields) {
      field(field);
    }
  }

  public TimeRangeExistQueryBuilder field(String field) {
    if (Strings.isEmpty(field)) {
      throw new IllegalArgumentException("supplied field is null or empty.");
    }
    this.fieldsBoosts.put(field, AbstractQueryBuilder.DEFAULT_BOOST);
    return this;
  }
  /** Adds a field to run the multi field against with a specific boost. */
  public TimeRangeExistQueryBuilder field(String field, float boost) {
    if (Strings.isEmpty(field)) {
      throw new IllegalArgumentException("supplied field is null or empty.");
    }
    checkNegativeBoost(boost);
    this.fieldsBoosts.put(field, boost);
    return this;
  }
  /** Add several fields to run the query against with a specific boost. */
  public TimeRangeExistQueryBuilder fields(Map<String, Float> fields) {
    for (float fieldBoost : fields.values()) {
      checkNegativeBoost(fieldBoost);
    }
    this.fieldsBoosts.putAll(fields);
    return this;
  }

  /** Read from a stream. */
  public TimeRangeExistQueryBuilder(StreamInput in) throws IOException {
    super(in);
    targetField = in.readString();
    targetValue = in.readString();
    minMatch = in.readInt();
    timeInterval = in.readLong();
    int size = in.readVInt();
    fieldsBoosts = new TreeMap<>();
    for (int i = 0; i < size; i++) {
      String field = in.readString();
      float boost = in.readFloat();
      checkNegativeBoost(boost);
      fieldsBoosts.put(field, boost);
    }
  }

  public static TimeRangeExistQueryBuilder fromXContent(XContentParser parser) throws IOException {
    String docId = null;
    String docField = null;
    Integer minMatch = null;
    Long timeInterval = null;
    String queryName = null;
    float boost = AbstractQueryBuilder.DEFAULT_BOOST;
    Map<String, Float> fieldsBoosts = new HashMap<>();

    XContentParser.Token token;
    String currentFieldName = null;
    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
      if (token == XContentParser.Token.FIELD_NAME) {
        currentFieldName = parser.currentName();
      } else if (FIELDS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
        if (token == XContentParser.Token.START_ARRAY) {
          while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            parseFieldAndBoost(parser, fieldsBoosts);
          }
        } else if (token.isValue()) {
          parseFieldAndBoost(parser, fieldsBoosts);
        } else {
          throw new ParsingException(
              parser.getTokenLocation(),
              "[" + NAME + "] query does not support [" + currentFieldName + "]");
        }
      } else if (token.isValue()) {
        if (TARGET_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
          docField = parser.text();
        }
        else if (TARGET_VALUE.match(currentFieldName, parser.getDeprecationHandler())) {
          docId = parser.text();
        } else if (MIN_MATCH_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
          minMatch = parser.intValue();
        } else if (TIME_INTERVAL_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
          timeInterval = parser.longValue();
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

    TimeRangeExistQueryBuilder builder =
        new TimeRangeExistQueryBuilder(docField, docId, minMatch, timeInterval);
    builder.fields(fieldsBoosts);
    builder.queryName(queryName);
    builder.boost(boost);
    return builder;
  }

  private static void parseFieldAndBoost(XContentParser parser, Map<String, Float> fieldsBoosts)
      throws IOException {
    String fField = null;
    Float fBoost = AbstractQueryBuilder.DEFAULT_BOOST;
    char[] fieldText = parser.textCharacters();
    int end = parser.textOffset() + parser.textLength();
    for (int i = parser.textOffset(); i < end; i++) {
      if (fieldText[i] == '^') {
        int relativeLocation = i - parser.textOffset();
        fField = new String(fieldText, parser.textOffset(), relativeLocation);
        fBoost =
            Float.parseFloat(
                new String(fieldText, i + 1, parser.textLength() - relativeLocation - 1));
        break;
      }
    }
    if (fField == null) {
      fField = parser.text();
    }
    fieldsBoosts.put(fField, fBoost);
  }

  @Override
  protected void doWriteTo(StreamOutput out) throws IOException {
    out.writeString(targetField);
    out.writeString(targetValue);
    out.writeInt(minMatch);
    out.writeLong(timeInterval);
    out.writeVInt(fieldsBoosts.size());
    for (Map.Entry<String, Float> fieldsEntry : fieldsBoosts.entrySet()) {
      out.writeString(fieldsEntry.getKey());
      out.writeFloat(fieldsEntry.getValue());
    }
  }

  @Override
  protected void doXContent(XContentBuilder builder, Params params) throws IOException {
    builder.startObject(NAME);
    builder.startArray(FIELDS_FIELD.getPreferredName());
    for (Map.Entry<String, Float> fieldEntry : this.fieldsBoosts.entrySet()) {
      builder.value(fieldEntry.getKey() + "^" + fieldEntry.getValue());
    }
    builder.endArray();
    builder.field(TARGET_FIELD.getPreferredName(), targetField);
    builder.field(TARGET_VALUE.getPreferredName(), targetValue);
    builder.field(MIN_MATCH_FIELD.getPreferredName(), minMatch);
    builder.field(TIME_INTERVAL_FIELD.getPreferredName(), timeInterval);
    printBoostAndQueryName(builder);
    builder.endObject();
  }

  @Override
  protected Query doToQuery(QueryShardContext context)  {
    return new TimeRangeExistQuery(fieldsBoosts, targetField, targetValue, minMatch, timeInterval);
  }

  @Override
  protected int doHashCode() {
    return Objects.hash(fieldsBoosts, targetField, targetValue, minMatch, timeInterval);
  }

  @Override
  protected boolean doEquals(TimeRangeExistQueryBuilder other) {
    return Objects.equals(fieldsBoosts, other.fieldsBoosts)
        && Objects.equals(targetField, other.targetField)
        && Objects.equals(targetValue, other.targetValue)
        && Objects.equals(minMatch, other.minMatch)
        && Objects.equals(timeInterval, other.timeInterval);
  }

  @Override
  public String getWriteableName() {
    return NAME;
  }
}
