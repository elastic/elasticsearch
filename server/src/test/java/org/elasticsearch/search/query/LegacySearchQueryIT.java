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

package org.elasticsearch.search.query;

import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.index.query.QueryBuilders.idsQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.typeQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;

public class LegacySearchQueryIT extends ESIntegTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testBasicQueryByIdMultiType() throws Exception {
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put("index.version.created", Version.V_5_6_0.id)));

        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").get();
        client().prepareIndex("test", "type2", "2").setSource("field1", "value2").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch().setQuery(idsQuery("type1", "type2").addIds("1", "2")).get();
        assertHitCount(searchResponse, 2L);
        assertThat(searchResponse.getHits().getHits().length, equalTo(2));

        searchResponse = client().prepareSearch().setQuery(idsQuery().addIds("1")).get();
        assertHitCount(searchResponse, 1L);
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));

        searchResponse = client().prepareSearch().setQuery(idsQuery().addIds("1", "2")).get();
        assertHitCount(searchResponse, 2L);
        assertThat(searchResponse.getHits().getHits().length, equalTo(2));

        searchResponse = client().prepareSearch().setQuery(idsQuery("type1").addIds("1", "2")).get();
        assertHitCount(searchResponse, 1L);
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));

        searchResponse = client().prepareSearch().setQuery(idsQuery(Strings.EMPTY_ARRAY).addIds("1")).get();
        assertHitCount(searchResponse, 1L);
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));

        searchResponse = client().prepareSearch().setQuery(idsQuery("type1", "type2", "type3").addIds("1", "2", "3", "4")).get();
        assertHitCount(searchResponse, 2L);
        assertThat(searchResponse.getHits().getHits().length, equalTo(2));
    }


    public void testTypeFilter() throws Exception {
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put("index.version.created", Version.V_5_6_0.id)));
        indexRandom(true, client().prepareIndex("test", "type1", "1").setSource("field1", "value1"),
                client().prepareIndex("test", "type2", "1").setSource("field1", "value1"),
                client().prepareIndex("test", "type1", "2").setSource("field1", "value1"),
                client().prepareIndex("test", "type2", "2").setSource("field1", "value1"),
                client().prepareIndex("test", "type2", "3").setSource("field1", "value1"));

        assertHitCount(client().prepareSearch().setQuery(typeQuery("type1")).get(), 2L);
        assertHitCount(client().prepareSearch().setQuery(typeQuery("type2")).get(), 3L);

        assertHitCount(client().prepareSearch().setTypes("type1").setQuery(matchAllQuery()).get(), 2L);
        assertHitCount(client().prepareSearch().setTypes("type2").setQuery(matchAllQuery()).get(), 3L);

        assertHitCount(client().prepareSearch().setTypes("type1", "type2").setQuery(matchAllQuery()).get(), 5L);
    }

}
