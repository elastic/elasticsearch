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

package org.elasticsearch.indices.template;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class LegacySimpleIndexTemplateIT extends ESIntegTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testIndexTemplateWithAliases() {

        client().admin().indices().preparePutTemplate("template_with_aliases")
                .setPatterns(Collections.singletonList("te*"))
                .addMapping("type1", "{\"type1\" : {\"properties\" : {\"value\" : {\"type\" : \"text\"}}}}", XContentType.JSON)
                .addAlias(new Alias("simple_alias"))
                .addAlias(new Alias("templated_alias-{index}"))
                .addAlias(new Alias("filtered_alias").filter("{\"type\":{\"value\":\"type2\"}}"))
                .addAlias(new Alias("complex_filtered_alias")
                        .filter(QueryBuilders.termsQuery("_type",  "typeX", "typeY", "typeZ")))
                .get();

        assertAcked(prepareCreate("test_index")
                .setSettings(Settings.builder().put("index.version.created", Version.V_5_6_0.id)) // allow for multiple version
                .addMapping("type1").addMapping("type2").addMapping("typeX").addMapping("typeY").addMapping("typeZ"));
        ensureGreen();

        client().prepareIndex("test_index", "type1", "1").setSource("field", "A value").get();
        client().prepareIndex("test_index", "type2", "2").setSource("field", "B value").get();
        client().prepareIndex("test_index", "typeX", "3").setSource("field", "C value").get();
        client().prepareIndex("test_index", "typeY", "4").setSource("field", "D value").get();
        client().prepareIndex("test_index", "typeZ", "5").setSource("field", "E value").get();

        GetAliasesResponse getAliasesResponse = client().admin().indices().prepareGetAliases().setIndices("test_index").get();
        assertThat(getAliasesResponse.getAliases().size(), equalTo(1));
        assertThat(getAliasesResponse.getAliases().get("test_index").size(), equalTo(4));

        refresh();

        SearchResponse searchResponse = client().prepareSearch("test_index").get();
        assertHitCount(searchResponse, 5L);

        searchResponse = client().prepareSearch("simple_alias").get();
        assertHitCount(searchResponse, 5L);

        searchResponse = client().prepareSearch("templated_alias-test_index").get();
        assertHitCount(searchResponse, 5L);

        searchResponse = client().prepareSearch("filtered_alias").get();
        assertHitCount(searchResponse, 1L);
        assertThat(searchResponse.getHits().getAt(0).getType(), equalTo("type2"));

        // Search the complex filter alias
        searchResponse = client().prepareSearch("complex_filtered_alias").get();
        assertHitCount(searchResponse, 3L);

        Set<String> types = new HashSet<>();
        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
            types.add(searchHit.getType());
        }
        assertThat(types.size(), equalTo(3));
        assertThat(types, containsInAnyOrder("typeX", "typeY", "typeZ"));
    }

}
