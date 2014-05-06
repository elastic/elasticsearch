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

package org.elasticsearch.index.fielddata;

import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

@ClusterScope(randomDynamicTemplates = false)
public class DisabledFieldDataFormatTests extends ElasticsearchIntegrationTest {

    @Override
    protected int numberOfReplicas() {
        return 0;
    }

    public void test() throws Exception {
        createIndex("test");
        ensureGreen();
        for (int i = 0; i < 10; ++i) {
            client().prepareIndex("test", "type", Integer.toString(i)).setSource("s", "value" + i).execute().actionGet();
        }

        refresh();

        // disable field data
        updateFormat("disabled");

        SearchResponse resp = null;
        // try to run something that relies on field data and make sure that it fails
        try {
            resp = client().prepareSearch("test").addAggregation(AggregationBuilders.terms("t").field("s")).execute().actionGet();
            assertFailures(resp);
        } catch (SearchPhaseExecutionException e) {
            // expected
        }

        // enable it again
        updateFormat("paged_bytes");

        // try to run something that relies on field data and make sure that it works
        resp = client().prepareSearch("test").addAggregation(AggregationBuilders.terms("t").field("s")).execute().actionGet();
        assertNoFailures(resp);

        // disable it again
        updateFormat("disabled");

        // this time, it should work because segments are already loaded
        resp = client().prepareSearch("test").addAggregation(AggregationBuilders.terms("t").field("s")).execute().actionGet();
        assertNoFailures(resp);

        // but add more docs and the new segment won't be loaded
        client().prepareIndex("test", "type", "-1").setSource("s", "value").execute().actionGet();
        refresh();
        try {
            resp = client().prepareSearch("test").addAggregation(AggregationBuilders.terms("t").field("s")).execute().actionGet();
            assertFailures(resp);
        } catch (SearchPhaseExecutionException e) {
            // expected
        }
    }

    private void updateFormat(String format) throws Exception {
        client().admin().indices().preparePutMapping("test").setType("type").setSource(
                XContentFactory.jsonBuilder().startObject().startObject("type")
                    .startObject("properties")
                        .startObject("s")
                            .field("type", "string")
                                .startObject("fielddata")
                                    .field("format", format)
                                .endObject()
                            .endObject()
                          .endObject()
                        .endObject()
                    .endObject()).execute().actionGet();
    }

}
