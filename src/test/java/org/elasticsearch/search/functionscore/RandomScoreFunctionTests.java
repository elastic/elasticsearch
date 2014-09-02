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

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.functionscore.random.RandomScoreFunctionBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.CoreMatchers;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;

import static com.google.common.collect.Lists.newArrayList;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.*;


public class RandomScoreFunctionTests extends ElasticsearchIntegrationTest {

    @Test
    public void testConsistentHitsWithSameSeed() throws Exception {
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
            final int seed = randomInt();
            String preference = randomRealisticUnicodeOfLengthBetween(1, 10); // at least one char!!
            // randomPreference should not start with '_' (reserved for known preference types (e.g. _shards, _primary)
            while (preference.startsWith("_")) {
                preference = randomRealisticUnicodeOfLengthBetween(1, 10);
            }
            int innerIters = scaledRandomIntBetween(2, 5);
            SearchHit[] hits = null;
            for (int i = 0; i < innerIters; i++) {
                SearchResponse searchResponse = client().prepareSearch()
                        .setSize(docCount) // get all docs otherwise we are prone to tie-breaking
                        .setPreference(preference)
                        .setQuery(functionScoreQuery(matchAllQuery(), randomFunction(seed)))
                        .execute().actionGet();
                assertThat("Failures " + Arrays.toString(searchResponse.getShardFailures()), searchResponse.getShardFailures().length, CoreMatchers.equalTo(0));
                final int hitCount = searchResponse.getHits().getHits().length;
                final SearchHit[] currentHits = searchResponse.getHits().getHits();
                ArrayUtil.timSort(currentHits, new Comparator<SearchHit>() {
                    @Override
                    public int compare(SearchHit o1, SearchHit o2) {
                        // for tie-breaking we have to resort here since if the score is
                        // identical we rely on collection order which might change.
                        int cmp = Float.compare(o1.getScore(), o2.getScore());
                        return cmp == 0 ? o1.getId().compareTo(o2.getId()) : cmp;
                    }
                });
                if (i == 0) {
                    assertThat(hits, nullValue());
                    hits = currentHits;
                } else {
                    assertThat(hits.length, equalTo(searchResponse.getHits().getHits().length));
                    for (int j = 0; j < hitCount; j++) {
                        assertThat("" + j, currentHits[j].score(), equalTo(hits[j].score()));
                        assertThat("" + j, currentHits[j].id(), equalTo(hits[j].id()));
                    }
                }

                // randomly change some docs to get them in different segments
                int numDocsToChange = randomIntBetween(20, 50);
                while (numDocsToChange > 0) {
                    int doc = randomInt(docCount-1);// watch out this is inclusive the max values!
                    index("test", "type", "" + doc, jsonBuilder().startObject().endObject());
                    --numDocsToChange;
                }
                flush();
                refresh();
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
    public void testSeedReportedInExplain() throws Exception {
        createIndex("test");
        ensureGreen();
        index("test", "type", "1", jsonBuilder().startObject().endObject());
        flush();
        refresh();

        int seed = 12345678;

        SearchResponse resp = client().prepareSearch("test")
            .setQuery(functionScoreQuery(matchAllQuery(), randomFunction(seed)))
            .setExplain(true)
            .get();
        assertNoFailures(resp);
        assertEquals(1, resp.getHits().totalHits());
        SearchHit firstHit = resp.getHits().getAt(0);
        assertThat(firstHit.explanation().toString(), containsString("" + seed));
    }

    public void testNoDocs() throws Exception {
        createIndex("test");
        ensureGreen();

        SearchResponse resp = client().prepareSearch("test")
            .setQuery(functionScoreQuery(matchAllQuery(), randomFunction(1234)))
            .get();
        assertNoFailures(resp);
        assertEquals(0, resp.getHits().totalHits());
    }

    @Test
    public void testScoreRange() throws Exception {
        // all random scores should be in range [0.0, 1.0]
        createIndex("test");
        ensureGreen();
        int docCount = randomIntBetween(100, 200);
        for (int i = 0; i < docCount; i++) {
            String id = randomRealisticUnicodeOfCodepointLengthBetween(1, 50);
            index("test", "type", id, jsonBuilder().startObject().endObject());
        }
        flush();
        refresh();
        int iters = scaledRandomIntBetween(10, 20);
        for (int i = 0; i < iters; ++i) {
            int seed = randomInt();
            SearchResponse searchResponse = client().prepareSearch()
                .setQuery(functionScoreQuery(matchAllQuery(), randomFunction(seed)))
                .setSize(docCount)
                .execute().actionGet();

            assertNoFailures(searchResponse);
            for (SearchHit hit : searchResponse.getHits().getHits()) {
                assertThat(hit.score(), allOf(greaterThanOrEqualTo(0.0f), lessThanOrEqualTo(1.0f)));
            }
        }
    }

    @Test
    @Ignore
    public void checkDistribution() throws Exception {
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
                    .setQuery(functionScoreQuery(matchAllQuery(), new RandomScoreFunctionBuilder()))
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
