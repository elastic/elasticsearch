/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.functionscore;

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.RandomScoreFunctionBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.ScoreAccessor;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.CoreMatchers;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.fieldValueFactorFunction;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.randomFunction;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.scriptFunction;
import static org.elasticsearch.script.MockScriptPlugin.NAME;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;

public class RandomScoreFunctionIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(CustomScriptPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

            scripts.put("log(doc['index'].value + (factor * _score))", vars -> scoringScript(vars, ScoreAccessor::doubleValue));
            scripts.put("log(doc['index'].value + (factor * _score.intValue()))", vars -> scoringScript(vars, ScoreAccessor::intValue));
            scripts.put("log(doc['index'].value + (factor * _score.longValue()))", vars -> scoringScript(vars, ScoreAccessor::longValue));
            scripts.put("log(doc['index'].value + (factor * _score.floatValue()))", vars -> scoringScript(vars, ScoreAccessor::floatValue));
            scripts.put(
                "log(doc['index'].value + (factor * _score.doubleValue()))",
                vars -> scoringScript(vars, ScoreAccessor::doubleValue)
            );
            return scripts;
        }

        static Double scoringScript(Map<String, Object> vars, Function<ScoreAccessor, Number> scoring) {
            Map<?, ?> doc = (Map) vars.get("doc");
            Double index = ((Number) ((ScriptDocValues<?>) doc.get("index")).get(0)).doubleValue();
            Double score = scoring.apply((ScoreAccessor) vars.get("_score")).doubleValue();
            Integer factor = (Integer) vars.get("factor");
            return Math.log(index + (factor * score));
        }
    }

    public void testConsistentHitsWithSameSeed() throws Exception {
        createIndex("test");
        ensureGreen(); // make sure we are done otherwise preference could change?
        int docCount = randomIntBetween(100, 200);
        for (int i = 0; i < docCount; i++) {
            index("test", "" + i, jsonBuilder().startObject().field("foo", i).endObject());
        }
        flush();
        refresh();
        int outerIters = scaledRandomIntBetween(10, 20);
        for (int o = 0; o < outerIters; o++) {
            final int seed = randomInt();
            String preference = randomRealisticUnicodeOfLengthBetween(1, 10); // at least one char!!
            // randomPreference should not start with '_' (reserved for known preference types (e.g. _shards)
            while (preference.startsWith("_")) {
                preference = randomRealisticUnicodeOfLengthBetween(1, 10);
            }
            int innerIters = scaledRandomIntBetween(2, 5);
            final SearchHit[][] hits = new SearchHit[1][];
            for (int i = 0; i < innerIters; i++) {
                final int finalI = i;
                assertResponse(
                    prepareSearch().setSize(docCount) // get all docs otherwise we are prone to tie-breaking
                        .setPreference(preference)
                        .setQuery(functionScoreQuery(matchAllQuery(), randomFunction().seed(seed).setField("foo"))),
                    response -> {
                        assertThat(
                            "Failures " + Arrays.toString(response.getShardFailures()),
                            response.getShardFailures().length,
                            CoreMatchers.equalTo(0)
                        );
                        final int hitCount = response.getHits().getHits().length;
                        final SearchHit[] currentHits = response.getHits().asUnpooled().getHits();
                        ArrayUtil.timSort(currentHits, (o1, o2) -> {
                            // for tie-breaking we have to resort here since if the score is
                            // identical we rely on collection order which might change.
                            int cmp = Float.compare(o1.getScore(), o2.getScore());
                            return cmp == 0 ? o1.getId().compareTo(o2.getId()) : cmp;
                        });
                        if (finalI == 0) {
                            assertThat(hits[0], nullValue());
                            hits[0] = currentHits;
                        } else {
                            assertThat(hits[0].length, equalTo(response.getHits().getHits().length));
                            for (int j = 0; j < hitCount; j++) {
                                assertThat("" + j, currentHits[j].getScore(), equalTo(hits[0][j].getScore()));
                                assertThat("" + j, currentHits[j].getId(), equalTo(hits[0][j].getId()));
                            }
                        }
                    }
                );

                // randomly change some docs to get them in different segments
                int numDocsToChange = randomIntBetween(20, 50);
                while (numDocsToChange > 0) {
                    int doc = randomInt(docCount - 1);// watch out this is inclusive the max values!
                    index("test", "" + doc, jsonBuilder().startObject().field("foo", doc).endObject());
                    --numDocsToChange;
                }
                flush();
                refresh();
            }
        }
    }

    public void testScoreAccessWithinScript() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                "body",
                "type=text",
                "index",
                "type=" + randomFrom("short", "float", "long", "integer", "double")
            )
        );

        int docCount = randomIntBetween(100, 200);
        for (int i = 0; i < docCount; i++) {
            prepareIndex("test").setId("" + i)
                // we add 1 to the index field to make sure that the scripts below never compute log(0)
                .setSource("body", randomFrom(Arrays.asList("foo", "bar", "baz")), "index", i + 1)
                .get();
        }
        refresh();

        Map<String, Object> params = new HashMap<>();
        params.put("factor", randomIntBetween(2, 4));

        // Test for accessing _score
        Script script = new Script(ScriptType.INLINE, NAME, "log(doc['index'].value + (factor * _score))", params);
        assertNoFailuresAndResponse(
            prepareSearch("test").setQuery(
                functionScoreQuery(
                    matchQuery("body", "foo"),
                    new FunctionScoreQueryBuilder.FilterFunctionBuilder[] {
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(fieldValueFactorFunction("index").factor(2)),
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(scriptFunction(script)) }
                )
            ),
            response -> {
                SearchHit firstHit = response.getHits().getAt(0);
                assertThat(firstHit.getScore(), greaterThan(1f));
            }
        );

        // Test for accessing _score.intValue()
        script = new Script(ScriptType.INLINE, NAME, "log(doc['index'].value + (factor * _score.intValue()))", params);
        assertNoFailuresAndResponse(
            prepareSearch("test").setQuery(
                functionScoreQuery(
                    matchQuery("body", "foo"),
                    new FunctionScoreQueryBuilder.FilterFunctionBuilder[] {
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(fieldValueFactorFunction("index").factor(2)),
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(scriptFunction(script)) }
                )
            ),
            response -> {
                SearchHit firstHit = response.getHits().getAt(0);
                assertThat(firstHit.getScore(), greaterThan(1f));
            }
        );

        // Test for accessing _score.longValue()
        script = new Script(ScriptType.INLINE, NAME, "log(doc['index'].value + (factor * _score.longValue()))", params);
        assertNoFailuresAndResponse(
            prepareSearch("test").setQuery(
                functionScoreQuery(
                    matchQuery("body", "foo"),
                    new FunctionScoreQueryBuilder.FilterFunctionBuilder[] {
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(fieldValueFactorFunction("index").factor(2)),
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(scriptFunction(script)) }
                )
            ),
            response -> {
                SearchHit firstHit = response.getHits().getAt(0);
                assertThat(firstHit.getScore(), greaterThan(1f));
            }
        );

        // Test for accessing _score.floatValue()
        script = new Script(ScriptType.INLINE, NAME, "log(doc['index'].value + (factor * _score.floatValue()))", params);
        assertNoFailuresAndResponse(
            prepareSearch("test").setQuery(
                functionScoreQuery(
                    matchQuery("body", "foo"),
                    new FunctionScoreQueryBuilder.FilterFunctionBuilder[] {
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(fieldValueFactorFunction("index").factor(2)),
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(scriptFunction(script)) }
                )
            ),
            response -> {
                SearchHit firstHit = response.getHits().getAt(0);
                assertThat(firstHit.getScore(), greaterThan(1f));
            }
        );

        // Test for accessing _score.doubleValue()
        script = new Script(ScriptType.INLINE, NAME, "log(doc['index'].value + (factor * _score.doubleValue()))", params);
        assertNoFailuresAndResponse(
            prepareSearch("test").setQuery(
                functionScoreQuery(
                    matchQuery("body", "foo"),
                    new FunctionScoreQueryBuilder.FilterFunctionBuilder[] {
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(fieldValueFactorFunction("index").factor(2)),
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(scriptFunction(script)) }
                )
            ),
            response -> {
                SearchHit firstHit = response.getHits().getAt(0);
                assertThat(firstHit.getScore(), greaterThan(1f));
            }
        );
    }

    public void testSeedReportedInExplain() throws Exception {
        createIndex("test");
        ensureGreen();
        index("test", "1", jsonBuilder().startObject().endObject());
        flush();
        refresh();

        int seed = 12345678;

        assertNoFailuresAndResponse(
            prepareSearch("test").setQuery(functionScoreQuery(matchAllQuery(), randomFunction().seed(seed).setField(SeqNoFieldMapper.NAME)))
                .setExplain(true),
            response -> {
                assertNoFailures(response);
                assertEquals(1, response.getHits().getTotalHits().value());
                SearchHit firstHit = response.getHits().getAt(0);
                assertThat(firstHit.getExplanation().toString(), containsString("" + seed));
            }
        );
    }

    public void testNoDocs() throws Exception {
        createIndex("test");
        ensureGreen();

        assertNoFailuresAndResponse(
            prepareSearch("test").setQuery(
                functionScoreQuery(matchAllQuery(), randomFunction().seed(1234).setField(SeqNoFieldMapper.NAME))
            ),
            response -> assertEquals(0, response.getHits().getTotalHits().value())
        );

        assertNoFailuresAndResponse(
            prepareSearch("test").setQuery(functionScoreQuery(matchAllQuery(), randomFunction())),
            response -> assertEquals(0, response.getHits().getTotalHits().value())
        );
    }

    public void testScoreRange() throws Exception {
        // all random scores should be in range [0.0, 1.0]
        createIndex("test");
        ensureGreen();
        int docCount = randomIntBetween(100, 200);
        for (int i = 0; i < docCount; i++) {
            String id = randomRealisticUnicodeOfCodepointLengthBetween(1, 50);
            index("test", id, jsonBuilder().startObject().endObject());
        }
        flush();
        refresh();
        int iters = scaledRandomIntBetween(10, 20);
        for (int i = 0; i < iters; ++i) {
            assertNoFailuresAndResponse(
                prepareSearch().setQuery(functionScoreQuery(matchAllQuery(), randomFunction())).setSize(docCount),
                response -> {
                    for (SearchHit hit : response.getHits().getHits()) {
                        assertThat(hit.getScore(), allOf(greaterThanOrEqualTo(0.0f), lessThanOrEqualTo(1.0f)));
                    }
                }
            );
        }
    }

    public void testSeeds() throws Exception {
        createIndex("test");
        ensureGreen();
        final int docCount = randomIntBetween(100, 200);
        for (int i = 0; i < docCount; i++) {
            index("test", "" + i, jsonBuilder().startObject().endObject());
        }
        flushAndRefresh();

        assertNoFailures(
            prepareSearch().setSize(docCount) // get all docs otherwise we are prone to tie-breaking
                .setQuery(functionScoreQuery(matchAllQuery(), randomFunction().seed(randomInt()).setField(SeqNoFieldMapper.NAME)))
        );

        assertNoFailures(
            prepareSearch().setSize(docCount) // get all docs otherwise we are prone to tie-breaking
                .setQuery(functionScoreQuery(matchAllQuery(), randomFunction().seed(randomLong()).setField(SeqNoFieldMapper.NAME)))
        );

        assertNoFailures(
            prepareSearch().setSize(docCount) // get all docs otherwise we are prone to tie-breaking
                .setQuery(
                    functionScoreQuery(
                        matchAllQuery(),
                        randomFunction().seed(randomRealisticUnicodeOfLengthBetween(10, 20)).setField(SeqNoFieldMapper.NAME)
                    )
                )
        );
    }

    public void checkDistribution() throws Exception {
        int count = 10000;

        assertAcked(prepareCreate("test"));
        ensureGreen();

        for (int i = 0; i < count; i++) {
            index("test", "" + i, jsonBuilder().startObject().endObject());
        }

        flush();
        refresh();

        int[] matrix = new int[count];

        for (int i = 0; i < count; i++) {

            assertResponse(
                prepareSearch().setQuery(functionScoreQuery(matchAllQuery(), new RandomScoreFunctionBuilder())),
                response -> matrix[Integer.valueOf(response.getHits().getAt(0).getId())]++
            );
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

        logger.info("max repeat: {}", maxRepeat);
        logger.info("avg repeat: {}", sumRepeat / (double) filled);
        logger.info("distribution: {}", filled / (double) count);

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
                logger.info("median: {}", i);
            } else if (percentile25 == 0) {
                logger.info("percentile_25: {}", i);
            } else if (percentile75 == 0) {
                logger.info("percentile_75: {}", i);
            }
            percentile50--;
            percentile25--;
            percentile75--;
        }

        logger.info("mean: {}", sum / (double) count);
    }
}
