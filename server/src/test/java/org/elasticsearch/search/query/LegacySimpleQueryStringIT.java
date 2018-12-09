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

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.simpleQueryStringQuery;
import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

public class LegacySimpleQueryStringIT extends ESIntegTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testExplicitAllFieldsRequested() throws Exception {
        String indexBody = copyToStringFromClasspath("/org/elasticsearch/search/query/all-query-index-with-all.json");
        prepareCreate("test")
                .setSource(indexBody, XContentType.JSON)
                // .setSettings(Settings.builder().put("index.version.created", Version.V_5_0_0.id)).get();
                .get();
        ensureGreen("test");

        List<IndexRequestBuilder> reqs = new ArrayList<>();
        reqs.add(client().prepareIndex("test", "doc", "1").setSource("f1", "foo", "f2", "eggplant"));
        indexRandom(true, false, reqs);

        SearchResponse resp = client().prepareSearch("test").setQuery(
                simpleQueryStringQuery("foo eggplant").defaultOperator(Operator.AND)).get();
        assertHitCount(resp, 0L);

        resp = client().prepareSearch("test").setQuery(
                simpleQueryStringQuery("foo eggplant").defaultOperator(Operator.AND).useAllFields(true)).get();
        SimpleQueryStringIT.assertHits(resp.getHits(), "1");
        assertHitCount(resp, 1L);
    }

}
