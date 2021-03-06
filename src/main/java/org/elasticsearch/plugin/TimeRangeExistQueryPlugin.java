/*
 * Copyright [2016] Doug Turnbull
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.elasticsearch.plugin;

import static java.util.Arrays.asList;

import java.util.List;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;

public class TimeRangeExistQueryPlugin extends Plugin implements SearchPlugin {

  public TimeRangeExistQueryPlugin(Settings settings) {}

  @Override
  public List<QuerySpec<?>> getQueries() {

    return asList(
        new QuerySpec<>(
            TimeRangeExistQueryBuilder.NAME,
            TimeRangeExistQueryBuilder::new,
            TimeRangeExistQueryBuilder::fromXContent));
  }
}
