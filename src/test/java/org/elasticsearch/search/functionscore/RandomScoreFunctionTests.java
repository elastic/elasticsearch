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
package org.elasticsearch.search.functionscore;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.CoreMatchers;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;

import static com.google.common.collect.Lists.newArrayList;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.nullValue;

public class RandomScoreFunctionTests extends ElasticsearchIntegrationTest {

    @Test
    public void consistentHitsWithSameSeed() throws Exception {
        createIndex("test");
        ensureGreen(); // make sure we are done otherwise preference could change?
        int docCount = randomIntBetween(100, 200);
        for (int i = 0; i < docCount; i++) {
            index("test", "type", "" + i, jsonBuilder().startObject().endObject());
        }
        flush();
        refresh();
        int outerIters = scaledRandomIntBetween(10, 20);
        for (int o = 0; o < outerIters; o++) {
            final long seed = randomLong();
            String preference = randomRealisticUnicodeOfLengthBetween(1, 10); // at least one char!!
            // randomPreference should not start with '_' (reserved for known preference types (e.g. _shards, _primary)
            while (preference.startsWith("_")) {
                preference = randomRealisticUnicodeOfLengthBetween(1, 10);
            }
            int innerIters = scaledRandomIntBetween(2, 5);
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
    public void testScoreAccessWithinScript() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type", "body", "type=string", "index", "type=" + randomFrom(new String[]{"short", "float", "long", "integer", "double"})));
        ensureYellow();

        int docCount = randomIntBetween(100, 200);
        for (int i = 0; i < docCount; i++) {
            client().prepareIndex("test", "type", "" + i).setSource("body", randomFrom(newArrayList("foo", "bar", "baz")), "index", i).get();
        }
        refresh();

        // Test for accessing _score
        SearchResponse resp = client().prepareSearch("test")
                .setQuery(functionScoreQuery(matchQuery("body", "foo"))
                        .add(fieldValueFactorFunction("index").factor(2))
                        .add(scriptFunction("log(doc['index'].value + (factor * _score))").param("factor", randomIntBetween(2, 4))))
                .get();
        assertNoFailures(resp);
        SearchHit firstHit = resp.getHits().getAt(0);
        assertThat(firstHit.getScore(), greaterThan(1f));

        // Test for accessing _score.intValue()
        resp = client().prepareSearch("test")
                .setQuery(functionScoreQuery(matchQuery("body", "foo"))
                        .add(fieldValueFactorFunction("index").factor(2))
                        .add(scriptFunction("log(doc['index'].value + (factor * _score.intValue()))")
                                .param("factor", randomIntBetween(2, 4))))
                .get();
        assertNoFailures(resp);
        firstHit = resp.getHits().getAt(0);
        assertThat(firstHit.getScore(), greaterThan(1f));

        // Test for accessing _score.longValue()
        resp = client().prepareSearch("test")
                .setQuery(functionScoreQuery(matchQuery("body", "foo"))
                        .add(fieldValueFactorFunction("index").factor(2))
                        .add(scriptFunction("log(doc['index'].value + (factor * _score.longValue()))")
                                .param("factor", randomIntBetween(2, 4))))
                .get();
        assertNoFailures(resp);
        firstHit = resp.getHits().getAt(0);
        assertThat(firstHit.getScore(), greaterThan(1f));

        // Test for accessing _score.floatValue()
        resp = client().prepareSearch("test")
                .setQuery(functionScoreQuery(matchQuery("body", "foo"))
                        .add(fieldValueFactorFunction("index").factor(2))
                        .add(scriptFunction("log(doc['index'].value + (factor * _score.floatValue()))")
                                .param("factor", randomIntBetween(2, 4))))
                .get();
        assertNoFailures(resp);
        firstHit = resp.getHits().getAt(0);
        assertThat(firstHit.getScore(), greaterThan(1f));

        // Test for accessing _score.doubleValue()
        resp = client().prepareSearch("test")
                .setQuery(functionScoreQuery(matchQuery("body", "foo"))
                        .add(fieldValueFactorFunction("index").factor(2))
                        .add(scriptFunction("log(doc['index'].value + (factor * _score.doubleValue()))")
                                .param("factor", randomIntBetween(2, 4))))
                .get();
        assertNoFailures(resp);
        firstHit = resp.getHits().getAt(0);
        assertThat(firstHit.getScore(), greaterThan(1f));
    }

    @Test
    @Ignore
    public void distribution() throws Exception {
        int count = 10000;

        assertAcked(prepareCreate("test"));
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
