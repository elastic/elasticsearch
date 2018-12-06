/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.search;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Collections;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class QueryParserHelperTests extends ESSingleNodeTestCase {

    private Integer maxClauseCountSetting;

    @Override
    public void setUp() throws Exception {
        this.maxClauseCountSetting = randomIntBetween(50, 100);
        super.setUp();
    }

    /**
     * Test that when
     * {@link QueryParserHelper#resolveMappingFields(QueryShardContext, java.util.Map, String)}
     * exceeds the limit defined by
     * {@link SearchModule#INDICES_MAX_CLAUSE_COUNT_SETTING}.
     */
    public void testLimitOnExpandedFields() throws Exception {
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        builder.startObject("_doc");
        builder.startObject("properties");
        for (int i = 0; i < maxClauseCountSetting + 1; i++) {
            builder.startObject("field" + i).field("type", "text").endObject();
        }
        builder.endObject(); // properties
        builder.endObject(); // type1
        builder.endObject();
        IndexService indexService = createIndex(
                "toomanyfields", Settings.builder()
                .put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), maxClauseCountSetting + 100).build(),
                "_doc", builder);
        client().prepareIndex("toomanyfields", "_doc", "1").setSource("field1", "foo bar baz").get();

        QueryShardContext queryShardContext = indexService.newQueryShardContext(randomInt(20), null, () -> {
            throw new UnsupportedOperationException();
        }, null);

        final String expectedWarning = "Field expansion matches too many fields, got: "+ (maxClauseCountSetting + 1) +". "
                + "This will be limited starting with version 7.0 of Elasticsearch. The limit will be detemined by the "
                + "`indices.query.bool.max_clause_count` setting which is currently set to " + maxClauseCountSetting + ". "
                + "You should look at lowering the maximum number of fields targeted by a query or increase the above limit "
                + "while being aware that this can negatively affect your clusters performance.";

        QueryParserHelper.resolveMappingField(queryShardContext, "*", 1.0f, true, false);
        assertWarnings(expectedWarning);

        QueryParserHelper.resolveMappingFields(queryShardContext, Collections.singletonMap("*", 1.0f));
        assertWarnings(expectedWarning);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(SearchModule.INDICES_MAX_CLAUSE_COUNT_SETTING.getKey(), this.maxClauseCountSetting).build();
    }
}
