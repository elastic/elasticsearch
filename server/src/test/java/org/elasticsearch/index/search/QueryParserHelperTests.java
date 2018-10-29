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
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Collections;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class QueryParserHelperTests extends ESSingleNodeTestCase {

    /**
     * Test that when {@link QueryParserHelper#resolveMappingFields(QueryShardContext, java.util.Map, String)} exceeds
     * the limit of 1024 fields, we emit a warning
     */
    public void testLimitOnExpandedFields() throws Exception {

        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        builder.startObject("type1");
        builder.startObject("properties");
        for (int i = 0; i < 1025; i++) {
            builder.startObject("field" + i).field("type", "text").endObject();
        }
        builder.endObject(); // properties
        builder.endObject(); // type1
        builder.endObject();
        IndexService indexService = createIndex("toomanyfields", Settings.builder()
                .put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), 1200).build(),
                "type1", builder);
        client().prepareIndex("toomanyfields", "type1", "1").setSource("field171", "foo bar baz").get();

        QueryShardContext queryShardContext = indexService.newQueryShardContext(
                randomInt(20), null, () -> { throw new UnsupportedOperationException(); }, null);

        QueryParserHelper.resolveMappingField(queryShardContext, "*", 1.0f, true, false);
        assertWarnings("Field expansion matches too many fields, got: 1025. A limit of 1024 will be enforced starting with "
                + "version 7.0 of Elasticsearch. Lowering the number of fields will be necessary before upgrading.");

        QueryParserHelper.resolveMappingFields(queryShardContext,Collections.singletonMap("*", 1.0f));
        assertWarnings("Field expansion matches too many fields, got: 1025. A limit of 1024 will be enforced starting with "
                + "version 7.0 of Elasticsearch. Lowering the number of fields will be necessary before upgrading.");
    }
}
