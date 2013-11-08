/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.functionscore;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.CoreMatchers;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.randomFunction;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class RandomScoreFunctionTests extends ElasticsearchIntegrationTest {

    @Test
    public void consistentHitsWithSameSeed() throws Exception {
        final int replicas = between(0, 2); // needed for green status!
        cluster().ensureAtLeastNumNodes(replicas + 1);
        assertAcked(client().admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.builder().put("index.number_of_shards", between(2, 5))
                                .put("index.number_of_replicas", replicas)
                                .build()));
        ensureGreen(); // make sure we are done otherwise preference could change?
        int docCount = atLeast(100);
        for (int i = 0; i < docCount; i++) {
            index("test", "type", "" + i, jsonBuilder().startObject().endObject());
        }
        flush();
        refresh();
        int outerIters = atLeast(10);
        for (int o = 0; o < outerIters; o++) {
            final long seed = randomLong();
            final String preference = randomRealisticUnicodeOfLengthBetween(1, 10); // at least one char!!
            int innerIters = atLeast(2);
            SearchHits hits = null;
            for (int i = 0; i < innerIters; i++) {
                SearchResponse searchResponse = client().prepareSearch()
                        .setPreference(preference)
                        .setQuery(functionScoreQuery(matchAllQuery(), randomFunction(seed)))
                        .execute().actionGet();
                assertThat("Failures " + Arrays.toString(searchResponse.getShardFailures()), searchResponse.getShardFailures().length, CoreMatchers.equalTo(0));
                int hitCount = searchResponse.getHits().getHits().length;
                if (i == 0) {
                    assertThat(hits, nullValue());
                    hits = searchResponse.getHits();
                } else {
                    assertThat(hits.getHits().length, equalTo(searchResponse.getHits().getHits().length));
                    for (int j = 0; j < hitCount; j++) {
                        assertThat(searchResponse.getHits().getAt(j).score(), equalTo(hits.getAt(j).score()));
                        assertThat(searchResponse.getHits().getAt(j).id(), equalTo(hits.getAt(j).id()));
                    }
                }
            }
        }
    }

    @Test
    @Ignore
    public void distribution() throws Exception {
        int count = 10000;

        prepareCreate("test").execute().actionGet();
        ensureGreen();

        for (int i = 0; i < count; i++) {
            index("test", "type", "" + i, jsonBuilder().startObject().endObject());
        }

        flush();
        refresh();

        int[] matrix = new int[count];

        for (int i = 0; i < count; i++) {

            SearchResponse searchResponse = client().prepareSearch()
                    .setQuery(functionScoreQuery(matchAllQuery(), randomFunction(System.nanoTime())))
                    .execute().actionGet();

            matrix[Integer.valueOf(searchResponse.getHits().getAt(0).id())]++;
        }

        int filled = 0;
        int maxRepeat = 0;
        int sumRepeat = 0;
        for (int i = 0; i < matrix.length; i++) {
            int value = matrix[i];
            sumRepeat += value;
            maxRepeat = Math.max(maxRepeat, value);
            if (value > 0) {
                filled++;
            }
        }

        System.out.println();
        System.out.println("max repeat: " + maxRepeat);
        System.out.println("avg repeat: " + sumRepeat / (double) filled);
        System.out.println("distribution: " + filled / (double) count);

        int percentile50 = filled / 2;
        int percentile25 = (filled / 4);
        int percentile75 = percentile50 + percentile25;

        int sum = 0;

        for (int i = 0; i < matrix.length; i++) {
            if (matrix[i] == 0) {
                continue;
            }
            sum += i * matrix[i];
            if (percentile50 == 0) {
                System.out.println("median: " + i);
            } else if (percentile25 == 0) {
                System.out.println("percentile_25: " + i);
            } else if (percentile75 == 0) {
                System.out.println("percentile_75: " + i);
            }
            percentile50--;
            percentile25--;
            percentile75--;
        }

        System.out.println("mean: " + sum / (double) count);

    }

}
