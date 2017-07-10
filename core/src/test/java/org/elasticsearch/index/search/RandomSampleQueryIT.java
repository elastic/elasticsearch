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

import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RandomSampleQueryBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, transportClientRatio = 0)
public class RandomSampleQueryIT extends ESIntegTestCase {

    private static final int NUM_DOCS = 10000;
    private static final int NUM_DOCS_SMALL = 100;
    @Before
    public void setUp() throws Exception {
        super.setUp();
        BulkRequestBuilder bulk = new BulkRequestBuilder(client(), BulkAction.INSTANCE);
        for (int i = 0; i < NUM_DOCS; i++) {
            bulk.add(client().prepareIndex("test", "test", Integer.toString(i)).setSource("field", i*2));
        }
        for (int i = 0; i < NUM_DOCS_SMALL; i++) {
            bulk.add(client().prepareIndex("test_small", "test", Integer.toString(i)).setSource("field", i*2));
        }
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        bulk.get();

    }

    public void testProbability() {
        for (int i = 10; i < 100; i+=10) {
            double p = ((double)i)/100;
            SearchResponse searchResponse = client()
                .prepareSearch("test")
                .setQuery(QueryBuilders.randomSampleQuery(p))
                .get();

            long hits = searchResponse.getHits().getTotalHits();
            double error = Math.abs((NUM_DOCS * p)/hits) / (NUM_DOCS * p);
            if (error > 0.15) {
                fail("Hit count was [" + hits + "], expected to be close to " + NUM_DOCS * p
                    + " (+/- 15% error). Error was " + error + ", p=" + p);
            }
        }
    }

    public void testProbabilitySmallIndex() {
        for (int i = 10; i < 100; i+=10) {
            double p = ((double)i)/100;
            SearchResponse searchResponse = client()
                .prepareSearch("test_small")
                .setQuery(QueryBuilders.randomSampleQuery(p))
                .get();

            long hits = searchResponse.getHits().getTotalHits();
            double error = Math.abs((NUM_DOCS * p)/hits) / (NUM_DOCS * p);
            if (error > 0.30) {
                fail("Hit count was [" + hits + "], expected to be close to " + NUM_DOCS * p
                    + " (+/- 30% error). Error was " + error + ", p=" + p);
            }
        }
    }

    public void testProbabilitySeeded() {
        for (int i = 10; i < 100; i+=10) {
            double p = ((double)i)/100;
            RandomSampleQueryBuilder builder = QueryBuilders.randomSampleQuery(p);
            builder.setSeed(randomInt());
            builder.setField("field");
            SearchResponse searchResponse = client()
                .prepareSearch("test")
                .setQuery(builder)
                .get();

            long hits = searchResponse.getHits().getTotalHits();
            double error = Math.abs((NUM_DOCS * p)/hits) / (NUM_DOCS * p);
            if (error > 0.15) {
                fail("Hit count was [" + hits + "], expected to be close to " + NUM_DOCS * p
                    + " (+/- 15% error). Error was " + error + ", p=" + p);
            }
        }
    }

    public void testProbabilitySmallIndexSeeded() {
        for (int i = 10; i < 100; i+=10) {
            double p = ((double)i)/100;
            RandomSampleQueryBuilder builder = QueryBuilders.randomSampleQuery(p);
            builder.setSeed(randomInt());
            builder.setField("field");
            SearchResponse searchResponse = client()
                .prepareSearch("test_small")
                .setQuery(builder)
                .get();

            long hits = searchResponse.getHits().getTotalHits();
            double error = Math.abs((NUM_DOCS * p)/hits) / (NUM_DOCS * p);
            if (error > 0.30) {
                fail("Hit count was [" + hits + "], expected to be close to " + NUM_DOCS * p
                    + " (+/- 30% error). Error was " + error + ", p=" + p);
            }
        }
    }

}
