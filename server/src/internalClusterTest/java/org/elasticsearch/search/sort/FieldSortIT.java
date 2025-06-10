/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.sort;

import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.fieldValueFactorFunction;
import static org.elasticsearch.script.MockScriptPlugin.NAME;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFirstHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitSize;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSecondHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasId;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class FieldSortIT extends ESIntegTestCase {
    public static class CustomScriptPlugin extends MockScriptPlugin {
        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();
            scripts.put("doc['number'].value", CustomScriptPlugin::sortDoubleScript);
            scripts.put("doc['keyword'].value", CustomScriptPlugin::sortStringScript);
            return scripts;
        }

        private static Double sortDoubleScript(Map<String, Object> vars) {
            Map<?, ?> doc = (Map) vars.get("doc");
            Double score = (Double) vars.get("_score");
            return ((Number) ((ScriptDocValues<?>) doc.get("number")).get(0)).doubleValue() + score;
        }

        private static String sortStringScript(Map<String, Object> vars) {
            Map<?, ?> doc = (Map) vars.get("doc");
            Double score = (Double) vars.get("_score");
            return ((ScriptDocValues<?>) doc.get("keyword")).get(0) + ",_score=" + score;
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(InternalSettingsPlugin.class, CustomScriptPlugin.class);
    }

    public void testIssue8226() {
        int numIndices = between(5, 10);
        final boolean useMapping = randomBoolean();
        for (int i = 0; i < numIndices; i++) {
            if (useMapping) {
                assertAcked(prepareCreate("test_" + i).addAlias(new Alias("test")).setMapping("entry", "type=long"));
            } else {
                assertAcked(prepareCreate("test_" + i).addAlias(new Alias("test")));
            }
            if (i > 0) {
                prepareIndex("test_" + i).setId("" + i).setSource("{\"entry\": " + i + "}", XContentType.JSON).get();
            }
        }
        refresh();
        // sort DESC
        assertNoFailuresAndResponse(
            prepareSearch().addSort(new FieldSortBuilder("entry").order(SortOrder.DESC).unmappedType(useMapping ? null : "long"))
                .setSize(10),
            response -> {
                logClusterState();
                Number previous = (Number) response.getHits().getHits()[0].getSourceAsMap().get("entry");
                for (int j = 1; j < response.getHits().getHits().length; j++) {
                    Number current = (Number) response.getHits().getHits()[j].getSourceAsMap().get("entry");
                    assertThat(response.toString(), current.intValue(), lessThan(previous.intValue()));
                    previous = current;
                }
            }
        );

        // sort ASC
        assertNoFailuresAndResponse(
            prepareSearch().addSort(new FieldSortBuilder("entry").order(SortOrder.ASC).unmappedType(useMapping ? null : "long"))
                .setSize(10),
            response -> {
                logClusterState();
                Number previous = (Number) response.getHits().getHits()[0].getSourceAsMap().get("entry");
                for (int j = 1; j < response.getHits().getHits().length; j++) {
                    Number current = (Number) response.getHits().getHits()[j].getSourceAsMap().get("entry");
                    assertThat(response.toString(), current.intValue(), greaterThan(previous.intValue()));
                    previous = current;
                }
            }
        );
    }

    public void testIssue6614() throws InterruptedException {
        List<IndexRequestBuilder> builders = new ArrayList<>();
        boolean strictTimeBasedIndices = randomBoolean();
        final int numIndices = randomIntBetween(2, 25); // at most 25 days in the month
        int docs = 0;
        for (int i = 0; i < numIndices; i++) {
            final String indexId = strictTimeBasedIndices ? "idx_" + i : "idx";
            if (strictTimeBasedIndices || i == 0) {
                createIndex(indexId);
            }
            final int numDocs = randomIntBetween(1, 23);  // hour of the day
            for (int j = 0; j < numDocs; j++) {
                builders.add(
                    prepareIndex(indexId).setSource(
                        "foo",
                        "bar",
                        "timeUpdated",
                        "2014/07/" + Strings.format("%02d", i + 1) + " " + Strings.format("%02d", j + 1) + ":00:00"
                    )
                );
            }
            indexRandom(true, builders);
            docs += builders.size();
            builders.clear();
        }
        final int finalDocs = docs;
        assertNoFailuresAndResponse(
            prepareSearch().setQuery(
                QueryBuilders.boolQuery()
                    .must(QueryBuilders.termQuery("foo", "bar"))
                    .must(QueryBuilders.rangeQuery("timeUpdated").gte("2014/0" + randomIntBetween(1, 7) + "/01"))
            ).addSort(new FieldSortBuilder("timeUpdated").order(SortOrder.ASC).unmappedType("date")).setSize(docs),
            allDocsResponse -> {
                final int numiters = randomIntBetween(1, 20);
                for (int i = 0; i < numiters; i++) {
                    assertNoFailuresAndResponse(
                        prepareSearch().setQuery(
                            QueryBuilders.boolQuery()
                                .must(QueryBuilders.termQuery("foo", "bar"))
                                .must(
                                    QueryBuilders.rangeQuery("timeUpdated")
                                        .gte("2014/" + Strings.format("%02d", randomIntBetween(1, 7)) + "/01")
                                )
                        )
                            .addSort(new FieldSortBuilder("timeUpdated").order(SortOrder.ASC).unmappedType("date"))
                            .setSize(scaledRandomIntBetween(1, finalDocs)),
                        response -> {
                            for (int j = 0; j < response.getHits().getHits().length; j++) {
                                assertThat(
                                    response.getHits().getHits()[j].getId(),
                                    equalTo(allDocsResponse.getHits().getHits()[j].getId())
                                );
                            }
                        }
                    );
                }
            }
        );
    }

    public void testTrackScores() throws Exception {
        assertAcked(indicesAdmin().prepareCreate("test").setMapping("svalue", "type=keyword").get());
        ensureGreen();
        index(
            "test",
            jsonBuilder().startObject().field("id", "1").field("svalue", "aaa").field("ivalue", 100).field("dvalue", 0.1).endObject()
        );
        index(
            "test",
            jsonBuilder().startObject().field("id", "2").field("svalue", "bbb").field("ivalue", 200).field("dvalue", 0.2).endObject()
        );
        refresh();

        assertResponse(prepareSearch().setQuery(matchAllQuery()).addSort("svalue", SortOrder.ASC), response -> {
            assertThat(response.getHits().getMaxScore(), equalTo(Float.NaN));
            for (SearchHit hit : response.getHits()) {
                assertThat(hit.getScore(), equalTo(Float.NaN));
            }
        });
        // now check with score tracking
        assertResponse(prepareSearch().setQuery(matchAllQuery()).addSort("svalue", SortOrder.ASC).setTrackScores(true), response -> {
            assertThat(response.getHits().getMaxScore(), not(equalTo(Float.NaN)));
            for (SearchHit hit : response.getHits()) {
                assertThat(hit.getScore(), not(equalTo(Float.NaN)));
            }
        });
    }

    public void testRandomSorting() throws IOException, InterruptedException, ExecutionException {
        Random random = random();
        assertAcked(
            prepareCreate("test").setMapping(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("sparse_bytes")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("dense_bytes")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        ensureGreen();

        TreeMap<BytesRef, String> sparseBytes = new TreeMap<>();
        TreeMap<BytesRef, String> denseBytes = new TreeMap<>();
        int numDocs = randomIntBetween(200, 300);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            String docId = Integer.toString(i);
            BytesRef ref = null;
            do {
                ref = new BytesRef(TestUtil.randomRealisticUnicodeString(random));
            } while (denseBytes.containsKey(ref));
            denseBytes.put(ref, docId);
            XContentBuilder src = jsonBuilder().startObject().field("dense_bytes", ref.utf8ToString());
            if (rarely()) {
                src.field("sparse_bytes", ref.utf8ToString());
                sparseBytes.put(ref, docId);
            }
            src.endObject();
            builders[i] = prepareIndex("test").setId(docId).setSource(src);
        }
        indexRandom(true, builders);
        {
            int size = between(1, denseBytes.size());
            assertNoFailuresAndResponse(
                prepareSearch("test").setQuery(matchAllQuery()).setSize(size).addSort("dense_bytes", SortOrder.ASC),
                response -> {
                    assertThat(response.getHits().getTotalHits().value, equalTo((long) numDocs));
                    assertThat(response.getHits().getHits().length, equalTo(size));
                    Set<Entry<BytesRef, String>> entrySet = denseBytes.entrySet();
                    Iterator<Entry<BytesRef, String>> iterator = entrySet.iterator();
                    for (int i = 0; i < size; i++) {
                        assertThat(iterator.hasNext(), equalTo(true));
                        Entry<BytesRef, String> next = iterator.next();
                        assertThat("pos: " + i, response.getHits().getAt(i).getId(), equalTo(next.getValue()));
                        assertThat(response.getHits().getAt(i).getSortValues()[0].toString(), equalTo(next.getKey().utf8ToString()));
                    }
                }
            );
        }
        if (sparseBytes.isEmpty() == false) {
            int size = between(1, sparseBytes.size());
            assertNoFailuresAndResponse(
                prepareSearch().setQuery(matchAllQuery())
                    .setPostFilter(QueryBuilders.existsQuery("sparse_bytes"))
                    .setSize(size)
                    .addSort("sparse_bytes", SortOrder.ASC),
                response -> {
                    assertThat(response.getHits().getTotalHits().value, equalTo((long) sparseBytes.size()));
                    assertThat(response.getHits().getHits().length, equalTo(size));
                    Set<Entry<BytesRef, String>> entrySet = sparseBytes.entrySet();
                    Iterator<Entry<BytesRef, String>> iterator = entrySet.iterator();
                    for (int i = 0; i < size; i++) {
                        assertThat(iterator.hasNext(), equalTo(true));
                        Entry<BytesRef, String> next = iterator.next();
                        assertThat(response.getHits().getAt(i).getId(), equalTo(next.getValue()));
                        assertThat(response.getHits().getAt(i).getSortValues()[0].toString(), equalTo(next.getKey().utf8ToString()));
                    }
                }
            );
        }
    }

    public void test3078() {
        assertAcked(indicesAdmin().prepareCreate("test").setMapping("field", "type=keyword").get());
        ensureGreen();

        for (int i = 1; i < 101; i++) {
            prepareIndex("test").setId(Integer.toString(i)).setSource("field", Integer.toString(i)).get();
        }
        refresh();
        assertResponse(
            prepareSearch("test").setQuery(matchAllQuery()).addSort(SortBuilders.fieldSort("field").order(SortOrder.ASC)),
            response -> {
                assertThat(response.getHits().getAt(0).getSortValues()[0].toString(), equalTo("1"));
                assertThat(response.getHits().getAt(1).getSortValues()[0].toString(), equalTo("10"));
                assertThat(response.getHits().getAt(2).getSortValues()[0].toString(), equalTo("100"));
            }
        );
        // reindex and refresh
        prepareIndex("test").setId(Integer.toString(1)).setSource("field", Integer.toString(1)).get();
        refresh();

        assertResponse(
            prepareSearch("test").setQuery(matchAllQuery()).addSort(SortBuilders.fieldSort("field").order(SortOrder.ASC)),
            response -> {
                assertThat(response.getHits().getAt(0).getSortValues()[0].toString(), equalTo("1"));
                assertThat(response.getHits().getAt(1).getSortValues()[0].toString(), equalTo("10"));
                assertThat(response.getHits().getAt(2).getSortValues()[0].toString(), equalTo("100"));
            }
        );
        // reindex - no refresh
        prepareIndex("test").setId(Integer.toString(1)).setSource("field", Integer.toString(1)).get();

        assertResponse(
            prepareSearch("test").setQuery(matchAllQuery()).addSort(SortBuilders.fieldSort("field").order(SortOrder.ASC)),
            response -> {
                assertThat(response.getHits().getAt(0).getSortValues()[0].toString(), equalTo("1"));
                assertThat(response.getHits().getAt(1).getSortValues()[0].toString(), equalTo("10"));
                assertThat(response.getHits().getAt(2).getSortValues()[0].toString(), equalTo("100"));
            }
        );
        // force merge
        forceMerge();
        refresh();

        prepareIndex("test").setId(Integer.toString(1)).setSource("field", Integer.toString(1)).get();
        assertResponse(
            prepareSearch("test").setQuery(matchAllQuery()).addSort(SortBuilders.fieldSort("field").order(SortOrder.ASC)),
            response -> {
                assertThat(response.getHits().getAt(0).getSortValues()[0].toString(), equalTo("1"));
                assertThat(response.getHits().getAt(1).getSortValues()[0].toString(), equalTo("10"));
                assertThat(response.getHits().getAt(2).getSortValues()[0].toString(), equalTo("100"));
            }
        );
        refresh();
        assertResponse(
            prepareSearch("test").setQuery(matchAllQuery()).addSort(SortBuilders.fieldSort("field").order(SortOrder.ASC)),
            response -> {
                assertThat(response.getHits().getAt(0).getSortValues()[0].toString(), equalTo("1"));
                assertThat(response.getHits().getAt(1).getSortValues()[0].toString(), equalTo("10"));
                assertThat(response.getHits().getAt(2).getSortValues()[0].toString(), equalTo("100"));
            }
        );
    }

    public void testScoreSortDirection() throws Exception {
        createIndex("test");
        ensureGreen();

        prepareIndex("test").setId("1").setSource("field", 2).get();
        prepareIndex("test").setId("2").setSource("field", 1).get();
        prepareIndex("test").setId("3").setSource("field", 0).get();

        refresh();

        assertResponse(
            prepareSearch("test").setQuery(
                QueryBuilders.functionScoreQuery(matchAllQuery(), ScoreFunctionBuilders.fieldValueFactorFunction("field"))
            ),
            response -> {
                assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
                assertThat(response.getHits().getAt(1).getScore(), Matchers.lessThan(response.getHits().getAt(0).getScore()));
                assertThat(response.getHits().getAt(1).getId(), equalTo("2"));
                assertThat(response.getHits().getAt(2).getScore(), Matchers.lessThan(response.getHits().getAt(1).getScore()));
                assertThat(response.getHits().getAt(2).getId(), equalTo("3"));
            }
        );
        assertResponse(
            prepareSearch("test").setQuery(
                QueryBuilders.functionScoreQuery(matchAllQuery(), ScoreFunctionBuilders.fieldValueFactorFunction("field"))
            ).addSort("_score", SortOrder.DESC),
            response -> {
                assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
                assertThat(response.getHits().getAt(1).getScore(), Matchers.lessThan(response.getHits().getAt(0).getScore()));
                assertThat(response.getHits().getAt(1).getId(), equalTo("2"));
                assertThat(response.getHits().getAt(2).getScore(), Matchers.lessThan(response.getHits().getAt(1).getScore()));
                assertThat(response.getHits().getAt(2).getId(), equalTo("3"));
            }
        );
        assertResponse(
            prepareSearch("test").setQuery(
                QueryBuilders.functionScoreQuery(matchAllQuery(), ScoreFunctionBuilders.fieldValueFactorFunction("field"))
            ).addSort("_score", SortOrder.DESC),
            response -> {
                assertThat(response.getHits().getAt(2).getId(), equalTo("3"));
                assertThat(response.getHits().getAt(1).getId(), equalTo("2"));
                assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
            }
        );
    }

    public void testScoreSortDirectionWithFunctionScore() throws Exception {
        createIndex("test");
        ensureGreen();

        prepareIndex("test").setId("1").setSource("field", 2).get();
        prepareIndex("test").setId("2").setSource("field", 1).get();
        prepareIndex("test").setId("3").setSource("field", 0).get();

        refresh();

        assertResponse(prepareSearch("test").setQuery(functionScoreQuery(matchAllQuery(), fieldValueFactorFunction("field"))), response -> {
            assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
            assertThat(response.getHits().getAt(1).getScore(), Matchers.lessThan(response.getHits().getAt(0).getScore()));
            assertThat(response.getHits().getAt(1).getId(), equalTo("2"));
            assertThat(response.getHits().getAt(2).getScore(), Matchers.lessThan(response.getHits().getAt(1).getScore()));
            assertThat(response.getHits().getAt(2).getId(), equalTo("3"));
        });
        assertResponse(
            prepareSearch("test").setQuery(functionScoreQuery(matchAllQuery(), fieldValueFactorFunction("field")))
                .addSort("_score", SortOrder.DESC),
            response -> {
                assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
                assertThat(response.getHits().getAt(1).getScore(), Matchers.lessThan(response.getHits().getAt(0).getScore()));
                assertThat(response.getHits().getAt(1).getId(), equalTo("2"));
                assertThat(response.getHits().getAt(2).getScore(), Matchers.lessThan(response.getHits().getAt(1).getScore()));
                assertThat(response.getHits().getAt(2).getId(), equalTo("3"));
            }
        );
        assertResponse(
            prepareSearch("test").setQuery(functionScoreQuery(matchAllQuery(), fieldValueFactorFunction("field")))
                .addSort("_score", SortOrder.DESC),
            response -> {
                assertThat(response.getHits().getAt(2).getId(), equalTo("3"));
                assertThat(response.getHits().getAt(1).getId(), equalTo("2"));
                assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
            }
        );
    }

    public void testIssue2986() {
        assertAcked(indicesAdmin().prepareCreate("test").setMapping("field1", "type=keyword").get());

        prepareIndex("test").setId("1").setSource("{\"field1\":\"value1\"}", XContentType.JSON).get();
        prepareIndex("test").setId("2").setSource("{\"field1\":\"value2\"}", XContentType.JSON).get();
        prepareIndex("test").setId("3").setSource("{\"field1\":\"value3\"}", XContentType.JSON).get();
        refresh();
        assertResponse(prepareSearch("test").setQuery(matchAllQuery()).setTrackScores(true).addSort("field1", SortOrder.ASC), response -> {
            for (SearchHit hit : response.getHits()) {
                assertFalse(Float.isNaN(hit.getScore()));
            }
        });
    }

    public void testIssue2991() {
        for (int i = 1; i < 4; i++) {
            try {
                indicesAdmin().prepareDelete("test").get();
            } catch (Exception e) {
                // ignore
            }
            assertAcked(indicesAdmin().prepareCreate("test").setMapping("tag", "type=keyword").get());
            ensureGreen();
            prepareIndex("test").setId("1").setSource("tag", "alpha").get();
            refresh();

            prepareIndex("test").setId("3").setSource("tag", "gamma").get();
            refresh();

            prepareIndex("test").setId("4").setSource("tag", "delta").get();

            refresh();
            prepareIndex("test").setId("2").setSource("tag", "beta").get();

            refresh();
            assertResponse(
                prepareSearch("test").setSize(2).setQuery(matchAllQuery()).addSort(SortBuilders.fieldSort("tag").order(SortOrder.ASC)),
                response -> {
                    assertHitCount(response, 4);
                    assertThat(response.getHits().getHits().length, equalTo(2));
                    assertFirstHit(response, hasId("1"));
                    assertSecondHit(response, hasId("2"));
                }
            );
            assertResponse(
                prepareSearch("test").setSize(2).setQuery(matchAllQuery()).addSort(SortBuilders.fieldSort("tag").order(SortOrder.DESC)),
                response -> {
                    assertHitCount(response, 4);
                    assertThat(response.getHits().getHits().length, equalTo(2));
                    assertFirstHit(response, hasId("3"));
                    assertSecondHit(response, hasId("4"));
                }
            );
        }
    }

    public void testSimpleSorts() throws Exception {
        Random random = random();
        assertAcked(
            prepareCreate("test").setMapping(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("str_value")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("boolean_value")
                    .field("type", "boolean")
                    .endObject()
                    .startObject("byte_value")
                    .field("type", "byte")
                    .endObject()
                    .startObject("short_value")
                    .field("type", "short")
                    .endObject()
                    .startObject("integer_value")
                    .field("type", "integer")
                    .endObject()
                    .startObject("long_value")
                    .field("type", "long")
                    .endObject()
                    .startObject("float_value")
                    .field("type", "float")
                    .endObject()
                    .startObject("half_float_value")
                    .field("type", "half_float")
                    .endObject()
                    .startObject("double_value")
                    .field("type", "double")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        ensureGreen();
        List<IndexRequestBuilder> builders = new ArrayList<>();
        final int numDocs = randomIntBetween(10, 127);
        for (int i = 0; i < numDocs; i++) {
            IndexRequestBuilder builder = prepareIndex("test").setId(Integer.toString(i))
                .setSource(
                    jsonBuilder().startObject()
                        .field("str_value", new String(new char[] { (char) (97 + i), (char) (97 + i) }))
                        .field("boolean_value", true)
                        .field("byte_value", i)
                        .field("short_value", i)
                        .field("integer_value", i)
                        .field("long_value", i)
                        .field("float_value", 0.1 * i)
                        .field("half_float_value", 0.1 * i)
                        .field("double_value", 0.1 * i)
                        .endObject()
                );
            builders.add(builder);
        }
        Collections.shuffle(builders, random);
        for (IndexRequestBuilder builder : builders) {
            builder.get();
            if (random.nextBoolean()) {
                if (random.nextInt(5) != 0) {
                    refresh();
                } else {
                    indicesAdmin().prepareFlush().get();
                }
            }

        }
        refresh();

        // STRING
        {
            int size = 1 + random.nextInt(numDocs);
            assertResponse(prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("str_value", SortOrder.ASC), response -> {
                assertHitCount(response, numDocs);
                assertThat(response.getHits().getHits().length, equalTo(size));
                for (int i = 0; i < size; i++) {
                    assertThat(response.getHits().getAt(i).getId(), equalTo(Integer.toString(i)));
                    assertThat(
                        response.getHits().getAt(i).getSortValues()[0].toString(),
                        equalTo(new String(new char[] { (char) (97 + i), (char) (97 + i) }))
                    );
                }
            });
        }
        {
            int size = 1 + random.nextInt(numDocs);
            assertResponse(prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("str_value", SortOrder.DESC), response -> {
                assertHitCount(response, numDocs);
                assertThat(response.getHits().getHits().length, equalTo(size));
                for (int i = 0; i < size; i++) {
                    int expectedValue = numDocs - 1 - i;
                    SearchHit hit = response.getHits().getAt(i);
                    assertThat(hit.getId(), equalTo(Integer.toString(expectedValue)));
                    assertThat(
                        hit.getSortValues()[0].toString(),
                        equalTo(new String(new char[] { (char) (97 + expectedValue), (char) (97 + expectedValue) }))
                    );
                }
                assertThat(response.toString(), not(containsString("error")));
            });
        }
        // BYTE
        {
            int size = 1 + random.nextInt(numDocs);
            assertResponse(prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("byte_value", SortOrder.ASC), response -> {
                assertHitCount(response, numDocs);
                assertThat(response.getHits().getHits().length, equalTo(size));
                for (int i = 0; i < size; i++) {
                    assertThat(response.getHits().getAt(i).getId(), equalTo(Integer.toString(i)));
                    assertThat(((Number) response.getHits().getAt(i).getSortValues()[0]).byteValue(), equalTo((byte) i));
                }
            });
        }
        {
            int size = 1 + random.nextInt(numDocs);
            assertResponse(prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("byte_value", SortOrder.DESC), response -> {
                assertHitCount(response, numDocs);
                assertThat(response.getHits().getHits().length, equalTo(size));
                for (int i = 0; i < size; i++) {
                    int expectedValue = numDocs - 1 - i;
                    SearchHit hit = response.getHits().getAt(i);
                    assertThat(hit.getId(), equalTo(Integer.toString(expectedValue)));
                    assertThat(((Number) hit.getSortValues()[0]).byteValue(), equalTo((byte) expectedValue));
                }
                assertThat(response.toString(), not(containsString("error")));
            });
        }
        // SHORT
        {
            int size = 1 + random.nextInt(numDocs);
            assertResponse(prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("short_value", SortOrder.ASC), response -> {
                assertHitCount(response, numDocs);
                assertThat(response.getHits().getHits().length, equalTo(size));
                for (int i = 0; i < size; i++) {
                    assertThat(response.getHits().getAt(i).getId(), equalTo(Integer.toString(i)));
                    assertThat(((Number) response.getHits().getAt(i).getSortValues()[0]).shortValue(), equalTo((short) i));
                }
            });
        }
        {
            int size = 1 + random.nextInt(numDocs);
            assertResponse(prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("short_value", SortOrder.DESC), response -> {
                assertHitCount(response, numDocs);
                assertThat(response.getHits().getHits().length, equalTo(size));
                for (int i = 0; i < size; i++) {
                    int expectedValue = numDocs - 1 - i;
                    SearchHit hit = response.getHits().getAt(i);
                    assertThat(hit.getId(), equalTo(Integer.toString(expectedValue)));
                    assertThat(((Number) hit.getSortValues()[0]).shortValue(), equalTo((short) expectedValue));
                }
                assertThat(response.toString(), not(containsString("error")));
            });
        }
        // INTEGER
        {
            int size = 1 + random.nextInt(numDocs);
            assertResponse(prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("integer_value", SortOrder.ASC), response -> {
                assertHitCount(response, numDocs);
                assertThat(response.getHits().getHits().length, equalTo(size));
                for (int i = 0; i < size; i++) {
                    assertThat(response.getHits().getAt(i).getId(), equalTo(Integer.toString(i)));
                    assertThat(((Number) response.getHits().getAt(i).getSortValues()[0]).intValue(), equalTo(i));
                }
                assertThat(response.toString(), not(containsString("error")));
            });
        }
        {
            int size = 1 + random.nextInt(numDocs);
            assertResponse(prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("integer_value", SortOrder.DESC), response -> {
                assertHitCount(response, numDocs);
                assertThat(response.getHits().getHits().length, equalTo(size));
                for (int i = 0; i < size; i++) {
                    int expectedValue = numDocs - 1 - i;
                    SearchHit hit = response.getHits().getAt(i);
                    assertThat(hit.getId(), equalTo(Integer.toString(expectedValue)));
                    assertThat(((Number) hit.getSortValues()[0]).intValue(), equalTo(expectedValue));
                }
                assertThat(response.toString(), not(containsString("error")));
            });
        }
        // LONG
        {
            int size = 1 + random.nextInt(numDocs);
            assertResponse(prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("long_value", SortOrder.ASC), response -> {
                assertHitCount(response, numDocs);
                assertThat(response.getHits().getHits().length, equalTo(size));
                for (int i = 0; i < size; i++) {
                    assertThat(response.getHits().getAt(i).getId(), equalTo(Integer.toString(i)));
                    assertThat(((Number) response.getHits().getAt(i).getSortValues()[0]).longValue(), equalTo((long) i));
                }

                assertThat(response.toString(), not(containsString("error")));
            });
        }
        {
            int size = 1 + random.nextInt(numDocs);
            assertResponse(prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("long_value", SortOrder.DESC), response -> {
                assertHitCount(response, numDocs);
                assertThat(response.getHits().getHits().length, equalTo(size));
                for (int i = 0; i < size; i++) {
                    int expectedValue = numDocs - 1 - i;
                    SearchHit hit = response.getHits().getAt(i);
                    assertThat(hit.getId(), equalTo(Integer.toString(expectedValue)));
                    assertThat(((Number) hit.getSortValues()[0]).longValue(), equalTo((long) expectedValue));
                }
                assertThat(response.toString(), not(containsString("error")));
            });
        }
        // FLOAT
        {
            int size = 1 + random.nextInt(numDocs);
            assertResponse(prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("float_value", SortOrder.ASC), response -> {
                assertHitCount(response, numDocs);
                assertThat(response.getHits().getHits().length, equalTo(size));
                for (int i = 0; i < size; i++) {
                    assertThat(response.getHits().getAt(i).getId(), equalTo(Integer.toString(i)));
                    assertThat(((Number) response.getHits().getAt(i).getSortValues()[0]).doubleValue(), closeTo(0.1d * i, 0.000001d));
                }
                assertThat(response.toString(), not(containsString("error")));
            });
        }
        {
            int size = 1 + random.nextInt(numDocs);
            assertResponse(prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("float_value", SortOrder.DESC), response -> {
                assertHitCount(response, numDocs);
                assertThat(response.getHits().getHits().length, equalTo(size));
                for (int i = 0; i < size; i++) {
                    int expectedValue = numDocs - 1 - i;
                    SearchHit hit = response.getHits().getAt(i);
                    assertThat(hit.getId(), equalTo(Integer.toString(expectedValue)));
                    assertThat(((Number) hit.getSortValues()[0]).doubleValue(), closeTo(0.1d * expectedValue, 0.000001d));
                }
                assertThat(response.toString(), not(containsString("error")));
            });
        }
        {
            // assert correctness of cast floats during sort (using numeric_type); no sort optimization is used
            int size = 1 + random.nextInt(numDocs);
            FieldSortBuilder sort = SortBuilders.fieldSort("float_value").order(SortOrder.ASC).setNumericType("double");
            assertResponse(prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort(sort), response -> {
                assertHitCount(response, numDocs);
                assertThat(response.getHits().getHits().length, equalTo(size));
                for (int i = 0; i < size; i++) {
                    assertThat(response.getHits().getAt(i).getId(), equalTo(Integer.toString(i)));
                    assertThat(((Number) response.getHits().getAt(i).getSortValues()[0]).doubleValue(), closeTo(0.1d * i, 0.000001d));
                }
                assertThat(response.toString(), not(containsString("error")));
            });
        }
        // HALF-FLOAT
        {
            int size = 1 + random.nextInt(numDocs);
            assertResponse(prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("half_float_value", SortOrder.ASC), response -> {
                assertHitCount(response, numDocs);
                assertThat(response.getHits().getHits().length, equalTo(size));
                for (int i = 0; i < size; i++) {
                    assertThat(response.getHits().getAt(i).getId(), equalTo(Integer.toString(i)));
                    assertThat(((Number) response.getHits().getAt(i).getSortValues()[0]).doubleValue(), closeTo(0.1d * i, 0.004d));
                }
                assertThat(response.toString(), not(containsString("error")));
            });
        }
        {
            int size = 1 + random.nextInt(numDocs);
            assertResponse(
                prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("half_float_value", SortOrder.DESC),
                response -> {
                    assertHitCount(response, numDocs);
                    assertThat(response.getHits().getHits().length, equalTo(size));
                    for (int i = 0; i < size; i++) {
                        int expectedValue = numDocs - 1 - i;
                        SearchHit hit = response.getHits().getAt(i);
                        assertThat(hit.getId(), equalTo(Integer.toString(expectedValue)));
                        assertThat(((Number) hit.getSortValues()[0]).doubleValue(), closeTo(0.1d * expectedValue, 0.004d));
                    }
                    assertThat(response.toString(), not(containsString("error")));
                }
            );
        }
        {
            // assert correctness of cast half_floats during sort (using numeric_type); no sort optimization is used
            int size = 1 + random.nextInt(numDocs);
            FieldSortBuilder sort = SortBuilders.fieldSort("half_float_value").order(SortOrder.ASC).setNumericType("double");
            assertResponse(prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort(sort), response -> {
                assertHitCount(response, numDocs);
                assertThat(response.getHits().getHits().length, equalTo(size));
                for (int i = 0; i < size; i++) {
                    assertThat(response.getHits().getAt(i).getId(), equalTo(Integer.toString(i)));
                    assertThat(((Number) response.getHits().getAt(i).getSortValues()[0]).doubleValue(), closeTo(0.1d * i, 0.004));
                }
                assertThat(response.toString(), not(containsString("error")));
            });
        }
        // DOUBLE
        {
            int size = 1 + random.nextInt(numDocs);
            assertResponse(prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("double_value", SortOrder.ASC), response -> {
                assertHitCount(response, numDocs);
                assertThat(response.getHits().getHits().length, equalTo(size));
                for (int i = 0; i < size; i++) {
                    assertThat(response.getHits().getAt(i).getId(), equalTo(Integer.toString(i)));
                    assertThat(((Number) response.getHits().getAt(i).getSortValues()[0]).doubleValue(), closeTo(0.1d * i, 0.000001d));
                }
                assertThat(response.toString(), not(containsString("error")));
            });
        }
        {
            int size = 1 + random.nextInt(numDocs);
            assertNoFailuresAndResponse(
                prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("double_value", SortOrder.DESC),
                response -> {
                    assertHitCount(response, numDocs);
                    assertThat(response.getHits().getHits().length, equalTo(size));
                    for (int i = 0; i < size; i++) {
                        int expectedValue = numDocs - 1 - i;
                        SearchHit hit = response.getHits().getAt(i);
                        assertThat(hit.getId(), equalTo(Integer.toString(expectedValue)));
                        assertThat(((Number) hit.getSortValues()[0]).doubleValue(), closeTo(0.1d * expectedValue, 0.000001d));
                    }
                }
            );
        }
    }

    public void testSortMissingNumbers() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("float_value")
                    .field("type", "float")
                    .endObject()
                    .startObject("int_value")
                    .field("type", "integer")
                    .endObject()
                    .startObject("byte_value")
                    .field("type", "byte")
                    .endObject()
                    .startObject("short_value")
                    .field("type", "short")
                    .endObject()
                    .startObject("long_value")
                    .field("type", "long")
                    .endObject()
                    .startObject("half_float_value")
                    .field("type", "half_float")
                    .endObject()
                    .startObject("double_value")
                    .field("type", "double")
                    .endObject()
                    .startObject("id")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        ensureGreen();

        int numDocs = randomIntBetween(50, 127);
        int missingRatio = 3;
        BulkRequestBuilder bulk = client().prepareBulk();

        List<Integer> docsWithValues = new ArrayList<>();
        int misCount = 0;
        for (int i = 0; i < numDocs; i++) {
            if (i % missingRatio == 0) {
                bulk.add(
                    prepareIndex("test").setId(Integer.toString(i))
                        .setSource(jsonBuilder().startObject().field("id", Integer.toString(i)).endObject())
                );
                misCount++;
            } else {
                byte byteValue = (byte) (i % 127);
                short shortValue = (short) (i * 2);
                int intValue = i;
                long longValue = i * 1000L;
                float floatValue = (float) (i * 0.1);
                float halfFloatValue = floatValue;
                double doubleValue = i * 0.001;
                bulk.add(
                    prepareIndex("test").setId(Integer.toString(i))
                        .setSource(
                            jsonBuilder().startObject()
                                .field("id", Integer.toString(i))
                                .field("byte_value", byteValue)
                                .field("short_value", shortValue)
                                .field("int_value", intValue)
                                .field("long_value", longValue)
                                .field("float_value", floatValue)
                                .field("half_float_value", halfFloatValue)
                                .field("double_value", doubleValue)
                                .endObject()
                        )
                );
                docsWithValues.add(i);
            }
        }
        assertNoFailures(bulk.get());
        refresh();
        final int missingCount = misCount;
        final int withValuesCount = docsWithValues.size();

        String[] fieldTypes = new String[] {
            "byte_value",
            "short_value",
            "int_value",
            "long_value",
            "float_value",
            "half_float_value",
            "double_value" };

        for (String fieldName : fieldTypes) {
            // Test sorting with missing _last (default behavior)
            assertNoFailuresAndResponse(
                prepareSearch().setSize(numDocs).setQuery(matchAllQuery()).addSort(SortBuilders.fieldSort(fieldName).order(SortOrder.ASC)),
                response -> {
                    assertEquals(numDocs, response.getHits().getHits().length);
                    for (int i = 0; i < docsWithValues.size(); i++) {
                        int expectedDocId = docsWithValues.get(i);
                        int actualDocId = Integer.parseInt(response.getHits().getAt(i).getId());
                        assertEquals("Field " + fieldName + ": wrong doc at position " + i, expectedDocId, actualDocId);
                    }
                    // all documents with missing values should appear at the end
                    for (int i = 0; i < missingCount; i++) {
                        int actualDocId = Integer.parseInt(response.getHits().getAt(withValuesCount + i).getId());
                        assertThat("Field " + fieldName + ": wrong missing doc at position " + i, actualDocId % missingRatio, equalTo(0));
                    }
                }
            );

            // Test sorting with missing _first
            assertNoFailuresAndResponse(
                prepareSearch().setSize(numDocs)
                    .setQuery(matchAllQuery())
                    .addSort(SortBuilders.fieldSort(fieldName).order(SortOrder.ASC).missing("_first")),
                response -> {
                    assertEquals(numDocs, response.getHits().getHits().length);
                    // all documents with missing values should appear at the beginning
                    for (int i = 0; i < missingCount; i++) {
                        int actualDocId = Integer.parseInt(response.getHits().getAt(i).getId());
                        assertThat("Field " + fieldName + ": wrong missing doc at position " + i, actualDocId % missingRatio, equalTo(0));
                    }
                    for (int i = 0; i < docsWithValues.size(); i++) {
                        int expectedDocId = docsWithValues.get(i);
                        int actualDocId = Integer.parseInt(response.getHits().getAt(i + missingCount).getId());
                        assertEquals("Field " + fieldName + ": wrong doc at position " + i, expectedDocId, actualDocId);
                    }
                }
            );
        }
    }

    public void testSortMissingStrings() throws IOException {
        assertAcked(
            prepareCreate("test").setMapping(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("value")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        ensureGreen();
        prepareIndex("test").setId("1").setSource(jsonBuilder().startObject().field("id", "1").field("value", "a").endObject()).get();

        prepareIndex("test").setId("2").setSource(jsonBuilder().startObject().field("id", "2").endObject()).get();

        prepareIndex("test").setId("3").setSource(jsonBuilder().startObject().field("id", "1").field("value", "c").endObject()).get();

        flush();
        refresh();

        // TODO: WTF?
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            throw new RuntimeException();
        }

        logger.info("--> sort with no missing (same as missing _last)");
        assertResponse(
            prepareSearch().setQuery(matchAllQuery()).addSort(SortBuilders.fieldSort("value").order(SortOrder.ASC)),
            response -> {
                assertThat(Arrays.toString(response.getShardFailures()), response.getFailedShards(), equalTo(0));

                assertThat(response.getHits().getTotalHits().value, equalTo(3L));
                assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
                assertThat(response.getHits().getAt(1).getId(), equalTo("3"));
                assertThat(response.getHits().getAt(2).getId(), equalTo("2"));
            }
        );
        logger.info("--> sort with missing _last");
        assertResponse(
            prepareSearch().setQuery(matchAllQuery()).addSort(SortBuilders.fieldSort("value").order(SortOrder.ASC).missing("_last")),
            response -> {
                assertThat(Arrays.toString(response.getShardFailures()), response.getFailedShards(), equalTo(0));

                assertThat(response.getHits().getTotalHits().value, equalTo(3L));
                assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
                assertThat(response.getHits().getAt(1).getId(), equalTo("3"));
                assertThat(response.getHits().getAt(2).getId(), equalTo("2"));
            }
        );
        logger.info("--> sort with missing _first");
        assertResponse(
            prepareSearch().setQuery(matchAllQuery()).addSort(SortBuilders.fieldSort("value").order(SortOrder.ASC).missing("_first")),
            response -> {
                assertThat(Arrays.toString(response.getShardFailures()), response.getFailedShards(), equalTo(0));

                assertThat(response.getHits().getTotalHits().value, equalTo(3L));
                assertThat(response.getHits().getAt(0).getId(), equalTo("2"));
                assertThat(response.getHits().getAt(1).getId(), equalTo("1"));
                assertThat(response.getHits().getAt(2).getId(), equalTo("3"));
            }
        );
        logger.info("--> sort with missing b");
        assertResponse(
            prepareSearch().setQuery(matchAllQuery()).addSort(SortBuilders.fieldSort("value").order(SortOrder.ASC).missing("b")),
            response -> {
                assertThat(Arrays.toString(response.getShardFailures()), response.getFailedShards(), equalTo(0));

                assertThat(response.getHits().getTotalHits().value, equalTo(3L));
                assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
                assertThat(response.getHits().getAt(1).getId(), equalTo("2"));
                assertThat(response.getHits().getAt(2).getId(), equalTo("3"));
            }
        );
    }

    public void testSortMissingDates() throws IOException {
        for (String type : List.of("date", "date_nanos")) {
            String index = "test_" + type;
            assertAcked(
                prepareCreate(index).setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("_doc")
                        .startObject("properties")
                        .startObject("mydate")
                        .field("type", type)
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
            );
            ensureGreen();
            prepareIndex(index).setId("1").setSource("mydate", "2021-01-01").get();
            prepareIndex(index).setId("2").setSource("mydate", "2021-02-01").get();
            prepareIndex(index).setId("3").setSource("other_field", "value").get();

            refresh();

            for (boolean withFormat : List.of(true, false)) {
                String format = null;
                if (withFormat) {
                    format = type.equals("date") ? "strict_date_optional_time" : "strict_date_optional_time_nanos";
                }

                assertResponse(
                    prepareSearch(index).addSort(SortBuilders.fieldSort("mydate").order(SortOrder.ASC).setFormat(format)),
                    response -> assertHitsInOrder(response, new String[] { "1", "2", "3" })
                );

                assertResponse(
                    prepareSearch(index).addSort(SortBuilders.fieldSort("mydate").order(SortOrder.ASC).missing("_first").setFormat(format)),
                    response -> assertHitsInOrder(response, new String[] { "3", "1", "2" })
                );

                assertResponse(
                    prepareSearch(index).addSort(SortBuilders.fieldSort("mydate").order(SortOrder.DESC).setFormat(format)),
                    response -> assertHitsInOrder(response, new String[] { "2", "1", "3" })
                );

                assertResponse(
                    prepareSearch(index).addSort(
                        SortBuilders.fieldSort("mydate").order(SortOrder.DESC).missing("_first").setFormat(format)
                    ),
                    response -> assertHitsInOrder(response, new String[] { "3", "2", "1" })
                );
            }
        }
    }

    /**
     * Sort across two indices with both "date" and "date_nanos" type using "numeric_type" set to "date_nanos"
     */
    public void testSortMissingDatesMixedTypes() throws IOException {
        for (String type : List.of("date", "date_nanos")) {
            String index = "test_" + type;
            assertAcked(
                prepareCreate(index).setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("_doc")
                        .startObject("properties")
                        .startObject("mydate")
                        .field("type", type)
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
            );

        }
        ensureGreen();

        prepareIndex("test_date").setId("1").setSource("mydate", "2021-01-01").get();
        prepareIndex("test_date").setId("2").setSource("mydate", "2021-02-01").get();
        prepareIndex("test_date").setId("3").setSource("other_field", 1).get();
        prepareIndex("test_date_nanos").setId("4").setSource("mydate", "2021-03-01").get();
        prepareIndex("test_date_nanos").setId("5").setSource("mydate", "2021-04-01").get();
        prepareIndex("test_date_nanos").setId("6").setSource("other_field", 2).get();
        refresh();

        for (boolean withFormat : List.of(true, false)) {
            String format = null;
            if (withFormat) {
                format = "strict_date_optional_time_nanos";
            }

            String index = "test*";
            assertResponse(
                prepareSearch(index).addSort(
                    SortBuilders.fieldSort("mydate").order(SortOrder.ASC).setFormat(format).setNumericType("date_nanos")
                ).addSort(SortBuilders.fieldSort("other_field").order(SortOrder.ASC)),
                response -> assertHitsInOrder(response, new String[] { "1", "2", "4", "5", "3", "6" })
            );

            assertResponse(
                prepareSearch(index).addSort(
                    SortBuilders.fieldSort("mydate").order(SortOrder.ASC).missing("_first").setFormat(format).setNumericType("date_nanos")
                ).addSort(SortBuilders.fieldSort("other_field").order(SortOrder.ASC)),
                response -> assertHitsInOrder(response, new String[] { "3", "6", "1", "2", "4", "5" })
            );

            assertResponse(
                prepareSearch(index).addSort(
                    SortBuilders.fieldSort("mydate").order(SortOrder.DESC).setFormat(format).setNumericType("date_nanos")
                ).addSort(SortBuilders.fieldSort("other_field").order(SortOrder.ASC)),
                response -> assertHitsInOrder(response, new String[] { "5", "4", "2", "1", "3", "6" })
            );

            assertResponse(
                prepareSearch(index).addSort(
                    SortBuilders.fieldSort("mydate").order(SortOrder.DESC).missing("_first").setFormat(format).setNumericType("date_nanos")
                ).addSort(SortBuilders.fieldSort("other_field").order(SortOrder.ASC)),
                response -> assertHitsInOrder(response, new String[] { "3", "6", "5", "4", "2", "1" })
            );
        }
    }

    private void assertHitsInOrder(SearchResponse response, String[] expectedIds) {
        SearchHit[] hits = response.getHits().getHits();
        assertEquals(expectedIds.length, hits.length);
        int i = 0;
        for (String id : expectedIds) {
            assertEquals(id, hits[i].getId());
            i++;
        }
    }

    public void testIgnoreUnmapped() throws Exception {
        createIndex("test");

        prepareIndex("test").setId("1")
            .setSource(jsonBuilder().startObject().field("id", "1").field("i_value", -1).field("d_value", -1.1).endObject())
            .get();

        logger.info("--> sort with an unmapped field, verify it fails");
        try {
            assertResponse(
                prepareSearch().setQuery(matchAllQuery()).addSort(SortBuilders.fieldSort("kkk")),
                response -> assertThat("Expected exception but returned with", response, nullValue())
            );
        } catch (SearchPhaseExecutionException e) {
            // we check that it's a parse failure rather than a different shard failure
            for (ShardSearchFailure shardSearchFailure : e.shardFailures()) {
                assertThat(shardSearchFailure.getCause().toString(), containsString("No mapping found for [kkk] in order to sort on"));
            }
        }

        assertNoFailures(prepareSearch().setQuery(matchAllQuery()).addSort(SortBuilders.fieldSort("kkk").unmappedType("keyword")));

        // nested field
        assertNoFailures(
            prepareSearch().setQuery(matchAllQuery())
                .addSort(
                    SortBuilders.fieldSort("nested.foo")
                        .unmappedType("keyword")
                        .setNestedSort(new NestedSortBuilder("nested").setNestedSort(new NestedSortBuilder("nested.foo")))
                )
        );

        // nestedQuery
        assertNoFailures(
            prepareSearch().setQuery(matchAllQuery())
                .addSort(
                    SortBuilders.fieldSort("nested.foo")
                        .unmappedType("keyword")
                        .setNestedSort(new NestedSortBuilder("nested").setFilter(QueryBuilders.termQuery("nested.foo", "abc")))
                )
        );
    }

    public void testSortMVField() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("long_values")
                    .field("type", "long")
                    .endObject()
                    .startObject("int_values")
                    .field("type", "integer")
                    .endObject()
                    .startObject("short_values")
                    .field("type", "short")
                    .endObject()
                    .startObject("byte_values")
                    .field("type", "byte")
                    .endObject()
                    .startObject("float_values")
                    .field("type", "float")
                    .endObject()
                    .startObject("double_values")
                    .field("type", "double")
                    .endObject()
                    .startObject("string_values")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        ensureGreen();

        prepareIndex("test").setId(Integer.toString(1))
            .setSource(
                jsonBuilder().startObject()
                    .array("long_values", 1L, 5L, 10L, 8L)
                    .array("int_values", 1, 5, 10, 8)
                    .array("short_values", 1, 5, 10, 8)
                    .array("byte_values", 1, 5, 10, 8)
                    .array("float_values", 1f, 5f, 10f, 8f)
                    .array("double_values", 1d, 5d, 10d, 8d)
                    .array("string_values", "01", "05", "10", "08")
                    .endObject()
            )
            .get();
        prepareIndex("test").setId(Integer.toString(2))
            .setSource(
                jsonBuilder().startObject()
                    .array("long_values", 11L, 15L, 20L, 7L)
                    .array("int_values", 11, 15, 20, 7)
                    .array("short_values", 11, 15, 20, 7)
                    .array("byte_values", 11, 15, 20, 7)
                    .array("float_values", 11f, 15f, 20f, 7f)
                    .array("double_values", 11d, 15d, 20d, 7d)
                    .array("string_values", "11", "15", "20", "07")
                    .endObject()
            )
            .get();
        prepareIndex("test").setId(Integer.toString(3))
            .setSource(
                jsonBuilder().startObject()
                    .array("long_values", 2L, 1L, 3L, -4L)
                    .array("int_values", 2, 1, 3, -4)
                    .array("short_values", 2, 1, 3, -4)
                    .array("byte_values", 2, 1, 3, -4)
                    .array("float_values", 2f, 1f, 3f, -4f)
                    .array("double_values", 2d, 1d, 3d, -4d)
                    .array("string_values", "02", "01", "03", "!4")
                    .endObject()
            )
            .get();

        refresh();

        assertResponse(prepareSearch().setQuery(matchAllQuery()).setSize(10).addSort("long_values", SortOrder.ASC), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(3L));
            assertThat(response.getHits().getHits().length, equalTo(3));

            assertThat(response.getHits().getAt(0).getId(), equalTo(Integer.toString(3)));
            assertThat(((Number) response.getHits().getAt(0).getSortValues()[0]).longValue(), equalTo(-4L));

            assertThat(response.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
            assertThat(((Number) response.getHits().getAt(1).getSortValues()[0]).longValue(), equalTo(1L));

            assertThat(response.getHits().getAt(2).getId(), equalTo(Integer.toString(2)));
            assertThat(((Number) response.getHits().getAt(2).getSortValues()[0]).longValue(), equalTo(7L));
        });
        assertResponse(prepareSearch().setQuery(matchAllQuery()).setSize(10).addSort("long_values", SortOrder.DESC), response -> {

            assertThat(response.getHits().getTotalHits().value, equalTo(3L));
            assertThat(response.getHits().getHits().length, equalTo(3));

            assertThat(response.getHits().getAt(0).getId(), equalTo(Integer.toString(2)));
            assertThat(((Number) response.getHits().getAt(0).getSortValues()[0]).longValue(), equalTo(20L));

            assertThat(response.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
            assertThat(((Number) response.getHits().getAt(1).getSortValues()[0]).longValue(), equalTo(10L));

            assertThat(response.getHits().getAt(2).getId(), equalTo(Integer.toString(3)));
            assertThat(((Number) response.getHits().getAt(2).getSortValues()[0]).longValue(), equalTo(3L));
        });
        assertResponse(
            prepareSearch().setQuery(matchAllQuery())
                .setSize(10)
                .addSort(SortBuilders.fieldSort("long_values").order(SortOrder.DESC).sortMode(SortMode.SUM)),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(3L));
                assertThat(response.getHits().getHits().length, equalTo(3));

                assertThat(response.getHits().getAt(0).getId(), equalTo(Integer.toString(2)));
                assertThat(((Number) response.getHits().getAt(0).getSortValues()[0]).longValue(), equalTo(53L));

                assertThat(response.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
                assertThat(((Number) response.getHits().getAt(1).getSortValues()[0]).longValue(), equalTo(24L));

                assertThat(response.getHits().getAt(2).getId(), equalTo(Integer.toString(3)));
                assertThat(((Number) response.getHits().getAt(2).getSortValues()[0]).longValue(), equalTo(2L));
            }
        );
        assertResponse(
            prepareSearch().setQuery(matchAllQuery())
                .setSize(10)
                .addSort(SortBuilders.fieldSort("long_values").order(SortOrder.DESC).sortMode(SortMode.AVG)),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(3L));
                assertThat(response.getHits().getHits().length, equalTo(3));

                assertThat(response.getHits().getAt(0).getId(), equalTo(Integer.toString(2)));
                assertThat(((Number) response.getHits().getAt(0).getSortValues()[0]).longValue(), equalTo(13L));

                assertThat(response.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
                assertThat(((Number) response.getHits().getAt(1).getSortValues()[0]).longValue(), equalTo(6L));

                assertThat(response.getHits().getAt(2).getId(), equalTo(Integer.toString(3)));
                assertThat(((Number) response.getHits().getAt(2).getSortValues()[0]).longValue(), equalTo(1L));
            }
        );
        assertResponse(
            prepareSearch().setQuery(matchAllQuery())
                .setSize(10)
                .addSort(SortBuilders.fieldSort("long_values").order(SortOrder.DESC).sortMode(SortMode.MEDIAN)),
            response -> {
                assertThat(response.getHits().getTotalHits().value, equalTo(3L));
                assertThat(response.getHits().getHits().length, equalTo(3));

                assertThat(response.getHits().getAt(0).getId(), equalTo(Integer.toString(2)));
                assertThat(((Number) response.getHits().getAt(0).getSortValues()[0]).longValue(), equalTo(13L));

                assertThat(response.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
                assertThat(((Number) response.getHits().getAt(1).getSortValues()[0]).longValue(), equalTo(7L));

                assertThat(response.getHits().getAt(2).getId(), equalTo(Integer.toString(3)));
                assertThat(((Number) response.getHits().getAt(2).getSortValues()[0]).longValue(), equalTo(2L));
            }
        );
        assertResponse(prepareSearch().setQuery(matchAllQuery()).setSize(10).addSort("int_values", SortOrder.ASC), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(3L));
            assertThat(response.getHits().getHits().length, equalTo(3));

            assertThat(response.getHits().getAt(0).getId(), equalTo(Integer.toString(3)));
            assertThat(((Number) response.getHits().getAt(0).getSortValues()[0]).intValue(), equalTo(-4));

            assertThat(response.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
            assertThat(((Number) response.getHits().getAt(1).getSortValues()[0]).intValue(), equalTo(1));

            assertThat(response.getHits().getAt(2).getId(), equalTo(Integer.toString(2)));
            assertThat(((Number) response.getHits().getAt(2).getSortValues()[0]).intValue(), equalTo(7));
        });
        assertResponse(prepareSearch().setQuery(matchAllQuery()).setSize(10).addSort("int_values", SortOrder.DESC), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(3L));
            assertThat(response.getHits().getHits().length, equalTo(3));

            assertThat(response.getHits().getAt(0).getId(), equalTo(Integer.toString(2)));
            assertThat(((Number) response.getHits().getAt(0).getSortValues()[0]).intValue(), equalTo(20));

            assertThat(response.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
            assertThat(((Number) response.getHits().getAt(1).getSortValues()[0]).intValue(), equalTo(10));

            assertThat(response.getHits().getAt(2).getId(), equalTo(Integer.toString(3)));
            assertThat(((Number) response.getHits().getAt(2).getSortValues()[0]).intValue(), equalTo(3));
        });
        assertResponse(prepareSearch().setQuery(matchAllQuery()).setSize(10).addSort("short_values", SortOrder.ASC), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(3L));
            assertThat(response.getHits().getHits().length, equalTo(3));

            assertThat(response.getHits().getAt(0).getId(), equalTo(Integer.toString(3)));
            assertThat(((Number) response.getHits().getAt(0).getSortValues()[0]).intValue(), equalTo(-4));

            assertThat(response.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
            assertThat(((Number) response.getHits().getAt(1).getSortValues()[0]).intValue(), equalTo(1));

            assertThat(response.getHits().getAt(2).getId(), equalTo(Integer.toString(2)));
            assertThat(((Number) response.getHits().getAt(2).getSortValues()[0]).intValue(), equalTo(7));
        });
        assertResponse(prepareSearch().setQuery(matchAllQuery()).setSize(10).addSort("short_values", SortOrder.DESC), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(3L));
            assertThat(response.getHits().getHits().length, equalTo(3));

            assertThat(response.getHits().getAt(0).getId(), equalTo(Integer.toString(2)));
            assertThat(((Number) response.getHits().getAt(0).getSortValues()[0]).intValue(), equalTo(20));

            assertThat(response.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
            assertThat(((Number) response.getHits().getAt(1).getSortValues()[0]).intValue(), equalTo(10));

            assertThat(response.getHits().getAt(2).getId(), equalTo(Integer.toString(3)));
            assertThat(((Number) response.getHits().getAt(2).getSortValues()[0]).intValue(), equalTo(3));
        });
        assertResponse(prepareSearch().setQuery(matchAllQuery()).setSize(10).addSort("byte_values", SortOrder.ASC), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(3L));
            assertThat(response.getHits().getHits().length, equalTo(3));

            assertThat(response.getHits().getAt(0).getId(), equalTo(Integer.toString(3)));
            assertThat(((Number) response.getHits().getAt(0).getSortValues()[0]).intValue(), equalTo(-4));

            assertThat(response.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
            assertThat(((Number) response.getHits().getAt(1).getSortValues()[0]).intValue(), equalTo(1));

            assertThat(response.getHits().getAt(2).getId(), equalTo(Integer.toString(2)));
            assertThat(((Number) response.getHits().getAt(2).getSortValues()[0]).intValue(), equalTo(7));
        });
        assertResponse(prepareSearch().setQuery(matchAllQuery()).setSize(10).addSort("byte_values", SortOrder.DESC), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(3L));
            assertThat(response.getHits().getHits().length, equalTo(3));

            assertThat(response.getHits().getAt(0).getId(), equalTo(Integer.toString(2)));
            assertThat(((Number) response.getHits().getAt(0).getSortValues()[0]).intValue(), equalTo(20));

            assertThat(response.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
            assertThat(((Number) response.getHits().getAt(1).getSortValues()[0]).intValue(), equalTo(10));

            assertThat(response.getHits().getAt(2).getId(), equalTo(Integer.toString(3)));
            assertThat(((Number) response.getHits().getAt(2).getSortValues()[0]).intValue(), equalTo(3));
        });
        assertResponse(prepareSearch().setQuery(matchAllQuery()).setSize(10).addSort("float_values", SortOrder.ASC), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(3L));
            assertThat(response.getHits().getHits().length, equalTo(3));

            assertThat(response.getHits().getAt(0).getId(), equalTo(Integer.toString(3)));
            assertThat(((Number) response.getHits().getAt(0).getSortValues()[0]).floatValue(), equalTo(-4f));

            assertThat(response.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
            assertThat(((Number) response.getHits().getAt(1).getSortValues()[0]).floatValue(), equalTo(1f));

            assertThat(response.getHits().getAt(2).getId(), equalTo(Integer.toString(2)));
            assertThat(((Number) response.getHits().getAt(2).getSortValues()[0]).floatValue(), equalTo(7f));
        });
        assertResponse(prepareSearch().setQuery(matchAllQuery()).setSize(10).addSort("float_values", SortOrder.DESC), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(3L));
            assertThat(response.getHits().getHits().length, equalTo(3));

            assertThat(response.getHits().getAt(0).getId(), equalTo(Integer.toString(2)));
            assertThat(((Number) response.getHits().getAt(0).getSortValues()[0]).floatValue(), equalTo(20f));

            assertThat(response.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
            assertThat(((Number) response.getHits().getAt(1).getSortValues()[0]).floatValue(), equalTo(10f));

            assertThat(response.getHits().getAt(2).getId(), equalTo(Integer.toString(3)));
            assertThat(((Number) response.getHits().getAt(2).getSortValues()[0]).floatValue(), equalTo(3f));
        });
        assertResponse(prepareSearch().setQuery(matchAllQuery()).setSize(10).addSort("double_values", SortOrder.ASC), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(3L));
            assertThat(response.getHits().getHits().length, equalTo(3));

            assertThat(response.getHits().getAt(0).getId(), equalTo(Integer.toString(3)));
            assertThat(((Number) response.getHits().getAt(0).getSortValues()[0]).doubleValue(), equalTo(-4d));

            assertThat(response.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
            assertThat(((Number) response.getHits().getAt(1).getSortValues()[0]).doubleValue(), equalTo(1d));

            assertThat(response.getHits().getAt(2).getId(), equalTo(Integer.toString(2)));
            assertThat(((Number) response.getHits().getAt(2).getSortValues()[0]).doubleValue(), equalTo(7d));
        });
        assertResponse(prepareSearch().setQuery(matchAllQuery()).setSize(10).addSort("double_values", SortOrder.DESC), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(3L));
            assertThat(response.getHits().getHits().length, equalTo(3));

            assertThat(response.getHits().getAt(0).getId(), equalTo(Integer.toString(2)));
            assertThat(((Number) response.getHits().getAt(0).getSortValues()[0]).doubleValue(), equalTo(20d));

            assertThat(response.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
            assertThat(((Number) response.getHits().getAt(1).getSortValues()[0]).doubleValue(), equalTo(10d));

            assertThat(response.getHits().getAt(2).getId(), equalTo(Integer.toString(3)));
            assertThat(((Number) response.getHits().getAt(2).getSortValues()[0]).doubleValue(), equalTo(3d));
        });
        assertResponse(prepareSearch().setQuery(matchAllQuery()).setSize(10).addSort("string_values", SortOrder.ASC), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(3L));
            assertThat(response.getHits().getHits().length, equalTo(3));

            assertThat(response.getHits().getAt(0).getId(), equalTo(Integer.toString(3)));
            assertThat(response.getHits().getAt(0).getSortValues()[0], equalTo("!4"));

            assertThat(response.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
            assertThat(response.getHits().getAt(1).getSortValues()[0], equalTo("01"));

            assertThat(response.getHits().getAt(2).getId(), equalTo(Integer.toString(2)));
            assertThat(response.getHits().getAt(2).getSortValues()[0], equalTo("07"));
        });
        assertResponse(prepareSearch().setQuery(matchAllQuery()).setSize(10).addSort("string_values", SortOrder.DESC), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(3L));
            assertThat(response.getHits().getHits().length, equalTo(3));

            assertThat(response.getHits().getAt(0).getId(), equalTo(Integer.toString(2)));
            assertThat(response.getHits().getAt(0).getSortValues()[0], equalTo("20"));

            assertThat(response.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
            assertThat(response.getHits().getAt(1).getSortValues()[0], equalTo("10"));

            assertThat(response.getHits().getAt(2).getId(), equalTo(Integer.toString(3)));
            assertThat(response.getHits().getAt(2).getSortValues()[0], equalTo("03"));
        });
    }

    public void testSortOnRareField() throws IOException {
        assertAcked(
            prepareCreate("test").setMapping(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("string_values")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        ensureGreen();
        prepareIndex("test").setId(Integer.toString(1))
            .setSource(jsonBuilder().startObject().array("string_values", "01", "05", "10", "08").endObject())
            .get();

        refresh();
        assertResponse(prepareSearch().setQuery(matchAllQuery()).setSize(3).addSort("string_values", SortOrder.DESC), response -> {
            assertThat(response.getHits().getHits().length, equalTo(1));

            assertThat(response.getHits().getAt(0).getId(), equalTo(Integer.toString(1)));
            assertThat(response.getHits().getAt(0).getSortValues()[0], equalTo("10"));
        });
        prepareIndex("test").setId(Integer.toString(2))
            .setSource(jsonBuilder().startObject().array("string_values", "11", "15", "20", "07").endObject())
            .get();
        for (int i = 0; i < 15; i++) {
            prepareIndex("test").setId(Integer.toString(300 + i))
                .setSource(jsonBuilder().startObject().array("some_other_field", "foobar").endObject())
                .get();
        }
        refresh();

        assertResponse(prepareSearch().setQuery(matchAllQuery()).setSize(2).addSort("string_values", SortOrder.DESC), response -> {

            assertThat(response.getHits().getHits().length, equalTo(2));

            assertThat(response.getHits().getAt(0).getId(), equalTo(Integer.toString(2)));
            assertThat(response.getHits().getAt(0).getSortValues()[0], equalTo("20"));

            assertThat(response.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
            assertThat(response.getHits().getAt(1).getSortValues()[0], equalTo("10"));
        });
        prepareIndex("test").setId(Integer.toString(3))
            .setSource(jsonBuilder().startObject().array("string_values", "02", "01", "03", "!4").endObject())
            .get();
        for (int i = 0; i < 15; i++) {
            prepareIndex("test").setId(Integer.toString(300 + i))
                .setSource(jsonBuilder().startObject().array("some_other_field", "foobar").endObject())
                .get();
        }
        refresh();

        assertResponse(prepareSearch().setQuery(matchAllQuery()).setSize(3).addSort("string_values", SortOrder.DESC), response -> {

            assertThat(response.getHits().getHits().length, equalTo(3));

            assertThat(response.getHits().getAt(0).getId(), equalTo(Integer.toString(2)));
            assertThat(response.getHits().getAt(0).getSortValues()[0], equalTo("20"));

            assertThat(response.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
            assertThat(response.getHits().getAt(1).getSortValues()[0], equalTo("10"));

            assertThat(response.getHits().getAt(2).getId(), equalTo(Integer.toString(3)));
            assertThat(response.getHits().getAt(2).getSortValues()[0], equalTo("03"));
        });
        for (int i = 0; i < 15; i++) {
            prepareIndex("test").setId(Integer.toString(300 + i))
                .setSource(jsonBuilder().startObject().array("some_other_field", "foobar").endObject())
                .get();
            refresh();
        }

        assertResponse(prepareSearch().setQuery(matchAllQuery()).setSize(3).addSort("string_values", SortOrder.DESC), response -> {

            assertThat(response.getHits().getHits().length, equalTo(3));

            assertThat(response.getHits().getAt(0).getId(), equalTo(Integer.toString(2)));
            assertThat(response.getHits().getAt(0).getSortValues()[0], equalTo("20"));

            assertThat(response.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
            assertThat(response.getHits().getAt(1).getSortValues()[0], equalTo("10"));

            assertThat(response.getHits().getAt(2).getId(), equalTo(Integer.toString(3)));
            assertThat(response.getHits().getAt(2).getSortValues()[0], equalTo("03"));
        });
    }

    public void testSortMetaField() throws Exception {
        updateClusterSettings(Settings.builder().put(IndicesService.INDICES_ID_FIELD_DATA_ENABLED_SETTING.getKey(), true));
        try {
            createIndex("test");
            ensureGreen();
            final int numDocs = randomIntBetween(10, 20);
            IndexRequestBuilder[] indexReqs = new IndexRequestBuilder[numDocs];
            for (int i = 0; i < numDocs; ++i) {
                indexReqs[i] = prepareIndex("test").setId(Integer.toString(i)).setSource();
            }
            indexRandom(true, indexReqs);

            SortOrder order = randomFrom(SortOrder.values());
            assertNoFailuresAndResponse(
                prepareSearch().setQuery(matchAllQuery()).setSize(randomIntBetween(1, numDocs + 5)).addSort("_id", order),
                response -> {
                    SearchHit[] hits = response.getHits().getHits();
                    BytesRef previous = order == SortOrder.ASC ? new BytesRef() : UnicodeUtil.BIG_TERM;
                    for (int i = 0; i < hits.length; ++i) {
                        String idString = hits[i].getId();
                        final BytesRef id = new BytesRef(idString);
                        assertEquals(idString, hits[i].getSortValues()[0]);
                        assertThat(previous, order == SortOrder.ASC ? lessThan(id) : greaterThan(id));
                        previous = id;
                    }
                }
            );
            // assertWarnings(ID_FIELD_DATA_DEPRECATION_MESSAGE);
        } finally {
            // unset cluster setting
            updateClusterSettings(Settings.builder().putNull(IndicesService.INDICES_ID_FIELD_DATA_ENABLED_SETTING.getKey()));
        }
    }

    /**
     * Test case for issue 6150: https://github.com/elastic/elasticsearch/issues/6150
     */
    public void testNestedSort() throws IOException, InterruptedException, ExecutionException {
        assertAcked(
            prepareCreate("test").setMapping(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("nested")
                    .field("type", "nested")
                    .startObject("properties")
                    .startObject("foo")
                    .field("type", "text")
                    .field("fielddata", true)
                    .startObject("fields")
                    .startObject("sub")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
                    .endObject()
                    .startObject("bar")
                    .field("type", "nested")
                    .startObject("properties")
                    .startObject("foo")
                    .field("type", "text")
                    .field("fielddata", true)
                    .startObject("fields")
                    .startObject("sub")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        ensureGreen();

        prepareIndex("test").setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .startArray("nested")
                    .startObject()
                    .field("foo", "bar bar")
                    .endObject()
                    .startObject()
                    .field("foo", "abc abc")
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();
        prepareIndex("test").setId("2")
            .setSource(
                jsonBuilder().startObject()
                    .startArray("nested")
                    .startObject()
                    .field("foo", "abc abc")
                    .endObject()
                    .startObject()
                    .field("foo", "cba bca")
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();
        refresh();

        // We sort on nested field

        assertNoFailuresAndResponse(
            prepareSearch().setQuery(matchAllQuery())
                .addSort(SortBuilders.fieldSort("nested.foo").setNestedSort(new NestedSortBuilder("nested")).order(SortOrder.DESC)),
            response -> {
                SearchHit[] hits = response.getHits().getHits();
                assertThat(hits.length, is(2));
                assertThat(hits[0].getSortValues().length, is(1));
                assertThat(hits[1].getSortValues().length, is(1));
                assertThat(hits[0].getSortValues()[0], is("cba"));
                assertThat(hits[1].getSortValues()[0], is("bar"));
            }
        );
        // We sort on nested fields with max_children limit
        assertNoFailuresAndResponse(
            prepareSearch().setQuery(matchAllQuery())
                .addSort(
                    SortBuilders.fieldSort("nested.foo")
                        .setNestedSort(new NestedSortBuilder("nested").setMaxChildren(1))
                        .order(SortOrder.DESC)
                ),
            response -> {
                SearchHit[] hits = response.getHits().getHits();
                assertThat(hits.length, is(2));
                assertThat(hits[0].getSortValues().length, is(1));
                assertThat(hits[1].getSortValues().length, is(1));
                assertThat(hits[0].getSortValues()[0], is("bar"));
                assertThat(hits[1].getSortValues()[0], is("abc"));

                {
                    SearchPhaseExecutionException exc = expectThrows(
                        SearchPhaseExecutionException.class,
                        prepareSearch().setQuery(matchAllQuery())
                            .addSort(
                                SortBuilders.fieldSort("nested.bar.foo")
                                    .setNestedSort(
                                        new NestedSortBuilder("nested").setNestedSort(new NestedSortBuilder("nested.bar").setMaxChildren(1))
                                    )
                                    .order(SortOrder.DESC)
                            )
                    );
                    assertThat(exc.toString(), containsString("max_children is only supported on top level of nested sort"));
                }
            }
        );
        // We sort on nested sub field
        assertNoFailuresAndResponse(
            prepareSearch().setQuery(matchAllQuery())
                .addSort(SortBuilders.fieldSort("nested.foo.sub").setNestedSort(new NestedSortBuilder("nested")).order(SortOrder.DESC)),
            response -> {
                SearchHit[] hits = response.getHits().getHits();
                assertThat(hits.length, is(2));
                assertThat(hits[0].getSortValues().length, is(1));
                assertThat(hits[1].getSortValues().length, is(1));
                assertThat(hits[0].getSortValues()[0], is("cba bca"));
                assertThat(hits[1].getSortValues()[0], is("bar bar"));
            }
        );
        // missing nested path
        SearchPhaseExecutionException exc = expectThrows(
            SearchPhaseExecutionException.class,
            prepareSearch().setQuery(matchAllQuery()).addSort(SortBuilders.fieldSort("nested.foo"))
        );
        assertThat(exc.toString(), containsString("it is mandatory to set the [nested] context"));
    }

    public void testSortDuelBetweenSingleShardAndMultiShardIndex() throws Exception {
        String sortField = "sortField";
        assertAcked(
            prepareCreate("test1").setSettings(
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(2, maximumNumberOfShards()))
            ).setMapping(sortField, "type=long")
        );
        assertAcked(
            prepareCreate("test2").setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1))
                .setMapping(sortField, "type=long")
        );

        for (String index : new String[] { "test1", "test2" }) {
            List<IndexRequestBuilder> docs = new ArrayList<>();
            for (int i = 0; i < 256; i++) {
                docs.add(prepareIndex(index).setId(Integer.toString(i)).setSource(sortField, i));
            }
            indexRandom(true, docs);
        }

        ensureSearchable("test1", "test2");
        SortOrder order = randomBoolean() ? SortOrder.ASC : SortOrder.DESC;
        int from = between(0, 256);
        int size = between(0, 256);
        assertNoFailuresAndResponse(
            prepareSearch("test1").setFrom(from).setSize(size).addSort(sortField, order),
            multiShardResponse -> assertNoFailuresAndResponse(
                prepareSearch("test2").setFrom(from).setSize(size).addSort(sortField, order),
                singleShardResponse -> {
                    assertThat(
                        multiShardResponse.getHits().getTotalHits().value,
                        equalTo(singleShardResponse.getHits().getTotalHits().value)
                    );
                    assertThat(multiShardResponse.getHits().getHits().length, equalTo(singleShardResponse.getHits().getHits().length));
                    for (int i = 0; i < multiShardResponse.getHits().getHits().length; i++) {
                        assertThat(
                            multiShardResponse.getHits().getAt(i).getSortValues()[0],
                            equalTo(singleShardResponse.getHits().getAt(i).getSortValues()[0])
                        );
                        assertThat(multiShardResponse.getHits().getAt(i).getId(), equalTo(singleShardResponse.getHits().getAt(i).getId()));
                    }
                }
            )
        );
    }

    public void testCustomFormat() throws Exception {
        // Use an ip field, which uses different internal/external
        // representations of values, to make sure values are both correctly
        // rendered and parsed (search_after)
        assertAcked(prepareCreate("test").setMapping("ip", "type=ip"));
        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource("ip", "192.168.1.7"),
            prepareIndex("test").setId("2").setSource("ip", "2001:db8::ff00:42:8329")
        );

        assertNoFailuresAndResponse(prepareSearch("test").addSort(SortBuilders.fieldSort("ip")), response -> {
            assertEquals(2, response.getHits().getTotalHits().value);
            assertArrayEquals(new String[] { "192.168.1.7" }, response.getHits().getAt(0).getSortValues());
            assertArrayEquals(new String[] { "2001:db8::ff00:42:8329" }, response.getHits().getAt(1).getSortValues());
        });
        assertNoFailuresAndResponse(
            prepareSearch("test").addSort(SortBuilders.fieldSort("ip")).searchAfter(new Object[] { "192.168.1.7" }),
            response -> {
                assertEquals(2, response.getHits().getTotalHits().value);
                assertEquals(1, response.getHits().getHits().length);
                assertArrayEquals(new String[] { "2001:db8::ff00:42:8329" }, response.getHits().getAt(0).getSortValues());
            }
        );
    }

    public void testScriptFieldSort() {
        assertAcked(prepareCreate("test").setMapping("keyword", "type=keyword", "number", "type=integer"));
        ensureGreen();
        final int numDocs = randomIntBetween(10, 20);
        IndexRequestBuilder[] indexReqs = new IndexRequestBuilder[numDocs];
        List<String> keywords = new ArrayList<>();
        for (int i = 0; i < numDocs; ++i) {
            indexReqs[i] = prepareIndex("test").setSource("number", i, "keyword", Integer.toString(i), "version", i + "." + i);
            keywords.add(Integer.toString(i));
        }
        Collections.sort(keywords);
        indexRandom(true, indexReqs);

        {
            Script script = new Script(ScriptType.INLINE, NAME, "doc['number'].value", Collections.emptyMap());
            assertResponse(
                prepareSearch().setQuery(matchAllQuery())
                    .setSize(randomIntBetween(1, numDocs + 5))
                    .addSort(SortBuilders.scriptSort(script, ScriptSortBuilder.ScriptSortType.NUMBER))
                    .addSort(SortBuilders.scoreSort()),
                response -> {
                    double expectedValue = 1; // start from 1 because it includes _score, 1.0f for all docs
                    for (SearchHit hit : response.getHits()) {
                        assertThat(hit.getSortValues().length, equalTo(2));
                        assertThat(hit.getSortValues()[0], equalTo(expectedValue++));
                        assertThat(hit.getSortValues()[1], equalTo(1f));
                    }
                }
            );
        }

        {
            Script script = new Script(ScriptType.INLINE, NAME, "doc['keyword'].value", Collections.emptyMap());
            assertResponse(
                prepareSearch().setQuery(matchAllQuery())
                    .setSize(randomIntBetween(1, numDocs + 5))
                    .addSort(SortBuilders.scriptSort(script, ScriptSortBuilder.ScriptSortType.STRING))
                    .addSort(SortBuilders.scoreSort()),
                response -> {
                    int expectedValue = 0;
                    for (SearchHit hit : response.getHits()) {
                        assertThat(hit.getSortValues().length, equalTo(2));
                        assertThat(hit.getSortValues()[0], equalTo(keywords.get(expectedValue++) + ",_score=1.0"));
                        assertThat(hit.getSortValues()[1], equalTo(1f));
                    }
                }
            );
        }
    }

    public void testFieldAlias() throws Exception {
        // Create two indices and add the field 'route_length_miles' as an alias in
        // one, and a concrete field in the other.
        assertAcked(prepareCreate("old_index").setMapping("distance", "type=double", "route_length_miles", "type=alias,path=distance"));
        assertAcked(prepareCreate("new_index").setMapping("route_length_miles", "type=double"));
        ensureGreen("old_index", "new_index");

        List<IndexRequestBuilder> builders = new ArrayList<>();
        builders.add(prepareIndex("old_index").setSource("distance", 42.0));
        builders.add(prepareIndex("old_index").setSource("distance", 50.5));
        builders.add(prepareIndex("new_index").setSource("route_length_miles", 100.2));
        indexRandom(true, true, builders);

        assertResponse(
            prepareSearch().setQuery(matchAllQuery()).setSize(builders.size()).addSort(SortBuilders.fieldSort("route_length_miles")),
            response -> {
                SearchHits hits = response.getHits();

                assertEquals(3, hits.getHits().length);
                assertEquals(42.0, hits.getAt(0).getSortValues()[0]);
                assertEquals(50.5, hits.getAt(1).getSortValues()[0]);
                assertEquals(100.2, hits.getAt(2).getSortValues()[0]);
            }
        );
    }

    public void testFieldAliasesWithMissingValues() throws Exception {
        // Create two indices and add the field 'route_length_miles' as an alias in
        // one, and a concrete field in the other.
        assertAcked(prepareCreate("old_index").setMapping("distance", "type=double", "route_length_miles", "type=alias,path=distance"));
        assertAcked(prepareCreate("new_index").setMapping("route_length_miles", "type=double"));
        ensureGreen("old_index", "new_index");

        List<IndexRequestBuilder> builders = new ArrayList<>();
        builders.add(prepareIndex("old_index").setSource("distance", 42.0));
        builders.add(prepareIndex("old_index").setSource(Collections.emptyMap()));
        builders.add(prepareIndex("new_index").setSource("route_length_miles", 100.2));
        indexRandom(true, true, builders);

        assertResponse(
            prepareSearch().setQuery(matchAllQuery())
                .setSize(builders.size())
                .addSort(SortBuilders.fieldSort("route_length_miles").missing(120.3)),
            response -> {
                SearchHits hits = response.getHits();

                assertEquals(3, hits.getHits().length);
                assertEquals(42.0, hits.getAt(0).getSortValues()[0]);
                assertEquals(100.2, hits.getAt(1).getSortValues()[0]);
                assertEquals(120.3, hits.getAt(2).getSortValues()[0]);
            }
        );
    }

    public void testCastNumericType() throws Exception {
        assertAcked(prepareCreate("index_double").setMapping("field", "type=double"));
        assertAcked(prepareCreate("index_long").setMapping("field", "type=long"));
        assertAcked(prepareCreate("index_float").setMapping("field", "type=float"));
        ensureGreen("index_double", "index_long", "index_float");

        List<IndexRequestBuilder> builders = new ArrayList<>();
        builders.add(prepareIndex("index_double").setSource("field", 12.6));
        builders.add(prepareIndex("index_long").setSource("field", 12));
        builders.add(prepareIndex("index_float").setSource("field", 12.1));
        indexRandom(true, true, builders);

        {
            assertResponse(
                prepareSearch().setQuery(matchAllQuery())
                    .setSize(builders.size())
                    .addSort(SortBuilders.fieldSort("field").setNumericType("long")),
                response -> {
                    SearchHits hits = response.getHits();

                    assertEquals(3, hits.getHits().length);
                    for (int i = 0; i < 3; i++) {
                        assertThat(hits.getAt(i).getSortValues()[0].getClass(), equalTo(Long.class));
                    }
                    assertEquals(12L, hits.getAt(0).getSortValues()[0]);
                    assertEquals(12L, hits.getAt(1).getSortValues()[0]);
                    assertEquals(12L, hits.getAt(2).getSortValues()[0]);
                }
            );
        }

        {
            assertResponse(
                prepareSearch().setQuery(matchAllQuery())
                    .setSize(builders.size())
                    .addSort(SortBuilders.fieldSort("field").setNumericType("double")),
                response -> {
                    SearchHits hits = response.getHits();
                    assertEquals(3, hits.getHits().length);
                    for (int i = 0; i < 3; i++) {
                        assertThat(hits.getAt(i).getSortValues()[0].getClass(), equalTo(Double.class));
                    }
                    assertEquals(12D, hits.getAt(0).getSortValues()[0]);
                    assertEquals(12.1D, (double) hits.getAt(1).getSortValues()[0], 0.001f);
                    assertEquals(12.6D, hits.getAt(2).getSortValues()[0]);
                }
            );
        }
    }

    public void testCastDate() throws Exception {
        assertAcked(prepareCreate("index_date").setMapping("field", "type=date"));
        assertAcked(prepareCreate("index_date_nanos").setMapping("field", "type=date_nanos"));
        ensureGreen("index_date", "index_date_nanos");

        List<IndexRequestBuilder> builders = new ArrayList<>();
        builders.add(prepareIndex("index_date").setSource("field", "2024-04-11T23:47:17"));
        builders.add(prepareIndex("index_date_nanos").setSource("field", "2024-04-11T23:47:16.854775807Z"));
        indexRandom(true, true, builders);

        {
            assertResponse(
                prepareSearch().setQuery(matchAllQuery()).setSize(2).addSort(SortBuilders.fieldSort("field").setNumericType("date")),
                response -> {
                    SearchHits hits = response.getHits();

                    assertEquals(2, hits.getHits().length);
                    for (int i = 0; i < 2; i++) {
                        assertThat(hits.getAt(i).getSortValues()[0].getClass(), equalTo(Long.class));
                    }
                    assertEquals(1712879236854L, hits.getAt(0).getSortValues()[0]);
                    assertEquals(1712879237000L, hits.getAt(1).getSortValues()[0]);
                }
            );
            assertResponse(
                prepareSearch().setMaxConcurrentShardRequests(1)
                    .setQuery(matchAllQuery())
                    .setSize(1)
                    .addSort(SortBuilders.fieldSort("field").setNumericType("date")),
                response -> {
                    SearchHits hits = response.getHits();

                    assertEquals(1, hits.getHits().length);
                    assertThat(hits.getAt(0).getSortValues()[0].getClass(), equalTo(Long.class));
                    assertEquals(1712879236854L, hits.getAt(0).getSortValues()[0]);
                }
            );
            assertResponse(
                prepareSearch().setMaxConcurrentShardRequests(1)
                    .setQuery(matchAllQuery())
                    .setSize(1)
                    .addSort(SortBuilders.fieldSort("field").order(SortOrder.DESC).setNumericType("date")),
                response -> {
                    SearchHits hits = response.getHits();

                    assertEquals(1, hits.getHits().length);
                    assertThat(hits.getAt(0).getSortValues()[0].getClass(), equalTo(Long.class));
                    assertEquals(1712879237000L, hits.getAt(0).getSortValues()[0]);
                }
            );
        }

        {
            assertResponse(
                prepareSearch().setQuery(matchAllQuery()).setSize(2).addSort(SortBuilders.fieldSort("field").setNumericType("date_nanos")),
                response -> {
                    SearchHits hits = response.getHits();
                    assertEquals(2, hits.getHits().length);
                    for (int i = 0; i < 2; i++) {
                        assertThat(hits.getAt(i).getSortValues()[0].getClass(), equalTo(Long.class));
                    }
                    assertEquals(1712879236854775807L, hits.getAt(0).getSortValues()[0]);
                    assertEquals(1712879237000000000L, hits.getAt(1).getSortValues()[0]);
                }
            );
            assertResponse(
                prepareSearch().setMaxConcurrentShardRequests(1)
                    .setQuery(matchAllQuery())
                    .setSize(1)
                    .addSort(SortBuilders.fieldSort("field").setNumericType("date_nanos")),
                response -> {
                    SearchHits hits = response.getHits();
                    assertEquals(1, hits.getHits().length);
                    assertThat(hits.getAt(0).getSortValues()[0].getClass(), equalTo(Long.class));
                    assertEquals(1712879236854775807L, hits.getAt(0).getSortValues()[0]);
                }
            );
            assertResponse(
                prepareSearch().setMaxConcurrentShardRequests(1)
                    .setQuery(matchAllQuery())
                    .setSize(1)
                    .addSort(SortBuilders.fieldSort("field").order(SortOrder.DESC).setNumericType("date_nanos")),
                response -> {
                    SearchHits hits = response.getHits();
                    assertEquals(1, hits.getHits().length);
                    assertThat(hits.getAt(0).getSortValues()[0].getClass(), equalTo(Long.class));
                    assertEquals(1712879237000000000L, hits.getAt(0).getSortValues()[0]);
                }
            );
        }

        {
            builders.clear();
            builders.add(prepareIndex("index_date").setSource("field", "1905-04-11T23:47:17"));
            indexRandom(true, true, builders);
            assertResponse(
                prepareSearch().setQuery(matchAllQuery()).setSize(1).addSort(SortBuilders.fieldSort("field").setNumericType("date_nanos")),
                response -> {
                    assertNotNull(response.getShardFailures());
                    assertThat(response.getShardFailures().length, equalTo(1));
                    assertThat(response.getShardFailures()[0].toString(), containsString("are before the epoch in 1970"));
                }
            );
        }

        {
            builders.clear();
            builders.add(prepareIndex("index_date").setSource("field", "2346-04-11T23:47:17"));
            indexRandom(true, true, builders);
            assertResponse(
                prepareSearch().setQuery(QueryBuilders.rangeQuery("field").gt("1970-01-01"))
                    .setSize(10)
                    .addSort(SortBuilders.fieldSort("field").setNumericType("date_nanos")),
                response -> {
                    assertNotNull(response.getShardFailures());
                    assertThat(response.getShardFailures().length, equalTo(1));
                    assertThat(response.getShardFailures()[0].toString(), containsString("are after 2262"));
                }
            );
        }
    }

    public void testCastNumericTypeExceptions() throws Exception {
        assertAcked(prepareCreate("index").setMapping("keyword", "type=keyword", "ip", "type=ip"));
        ensureGreen("index");
        for (String invalidField : new String[] { "keyword", "ip" }) {
            for (String numericType : new String[] { "long", "double", "date", "date_nanos" }) {
                ElasticsearchException exc = expectThrows(
                    ElasticsearchException.class,
                    prepareSearch().setQuery(matchAllQuery()).addSort(SortBuilders.fieldSort(invalidField).setNumericType(numericType))
                );
                assertThat(exc.status(), equalTo(RestStatus.BAD_REQUEST));
                assertThat(exc.getDetailedMessage(), containsString("[numeric_type] option cannot be set on a non-numeric field"));
            }
        }
    }

    public void testLongSortOptimizationCorrectResults() {
        assertAcked(
            prepareCreate("test1").setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2))
                .setMapping("long_field", "type=long")
        );

        BulkRequestBuilder bulkBuilder = client().prepareBulk();
        for (int i = 1; i <= 7000; i++) {
            if (i % 3500 == 0) {
                bulkBuilder.get();
                bulkBuilder = client().prepareBulk();
            }
            String source = "{\"long_field\":" + randomLong() + "}";
            bulkBuilder.add(prepareIndex("test1").setId(Integer.toString(i)).setSource(source, XContentType.JSON));
        }
        refresh();

        // *** 1. sort DESC on long_field
        assertNoFailuresAndResponse(
            prepareSearch().addSort(new FieldSortBuilder("long_field").order(SortOrder.DESC)).setSize(10),
            response -> {
                long previousLong = Long.MAX_VALUE;
                for (int i = 0; i < response.getHits().getHits().length; i++) {
                    // check the correct sort order
                    SearchHit hit = response.getHits().getHits()[i];
                    long currentLong = (long) hit.getSortValues()[0];
                    assertThat("sort order is incorrect", currentLong, lessThanOrEqualTo(previousLong));
                    previousLong = currentLong;
                }
            }
        );

        // *** 2. sort ASC on long_field
        assertNoFailuresAndResponse(
            prepareSearch().addSort(new FieldSortBuilder("long_field").order(SortOrder.ASC)).setSize(10),
            response -> {
                long previousLong = Long.MIN_VALUE;
                for (int i = 0; i < response.getHits().getHits().length; i++) {
                    // check the correct sort order
                    SearchHit hit = response.getHits().getHits()[i];
                    long currentLong = (long) hit.getSortValues()[0];
                    assertThat("sort order is incorrect", currentLong, greaterThanOrEqualTo(previousLong));
                    previousLong = currentLong;
                }
            }
        );
    }

    public void testSortMixedFieldTypes() {
        assertAcked(
            prepareCreate("index_long").setMapping("foo", "type=long"),
            prepareCreate("index_integer").setMapping("foo", "type=integer"),
            prepareCreate("index_double").setMapping("foo", "type=double"),
            prepareCreate("index_keyword").setMapping("foo", "type=keyword")
        );

        prepareIndex("index_long").setId("1").setSource("foo", "123").get();
        prepareIndex("index_integer").setId("1").setSource("foo", "123").get();
        prepareIndex("index_double").setId("1").setSource("foo", "123").get();
        prepareIndex("index_keyword").setId("1").setSource("foo", "123").get();
        refresh();

        { // mixing long and integer types is ok, as we convert integer sort to long sort
            assertNoFailures(prepareSearch("index_long", "index_integer").addSort(new FieldSortBuilder("foo")).setSize(10));
        }

        String errMsg = "Can't sort on field [foo]; the field has incompatible sort types";

        { // mixing long and double types is not allowed
            SearchPhaseExecutionException exc = expectThrows(
                SearchPhaseExecutionException.class,
                prepareSearch("index_long", "index_double").addSort(new FieldSortBuilder("foo")).setSize(10)
            );
            assertThat(exc.getCause().toString(), containsString(errMsg));
        }

        { // mixing long and keyword types is not allowed
            SearchPhaseExecutionException exc = expectThrows(
                SearchPhaseExecutionException.class,
                prepareSearch("index_long", "index_keyword").addSort(new FieldSortBuilder("foo")).setSize(10)
            );
            assertThat(exc.getCause().toString(), containsString(errMsg));
        }
    }

    public void testMixedIntAndLongSortTypes() {
        assertAcked(
            prepareCreate("index_long").setMapping("field1", "type=long", "field2", "type=long"),
            prepareCreate("index_integer").setMapping("field1", "type=integer", "field2", "type=integer"),
            prepareCreate("index_short").setMapping("field1", "type=short", "field2", "type=short"),
            prepareCreate("index_byte").setMapping("field1", "type=byte", "field2", "type=byte")
        );

        for (int i = 0; i < 5; i++) {
            prepareIndex("index_long").setId(String.valueOf(i)).setSource("field1", i).get(); // missing field2 sorts last
            prepareIndex("index_integer").setId(String.valueOf(i)).setSource("field1", i).get(); // missing field2 sorts last
            prepareIndex("index_short").setId(String.valueOf(i)).setSource("field1", i, "field2", i * 10).get();
            prepareIndex("index_byte").setId(String.valueOf(i)).setSource("field1", i, "field2", i).get();
        }
        refresh();

        Object[] searchAfter = null;
        int[] expectedHitSizes = { 8, 8, 4 };
        Object[][] expectedLastDocValues = {
            new Object[] { 1L, 9223372036854775807L },
            new Object[] { 3L, 9223372036854775807L },
            new Object[] { 4L, 9223372036854775807L } };

        for (int i = 0; i < 3; i++) {
            SearchRequestBuilder request = prepareSearch("index_long", "index_integer", "index_short", "index_byte").setSize(8)
                .addSort(new FieldSortBuilder("field1"))
                .addSort(new FieldSortBuilder("field2"));
            if (searchAfter != null) {
                request.searchAfter(searchAfter);
            }
            SearchResponse response = request.get();
            assertHitSize(response, expectedHitSizes[i]);
            Object[] lastDocSortValues = response.getHits().getAt(response.getHits().getHits().length - 1).getSortValues();
            assertThat(lastDocSortValues, equalTo(expectedLastDocValues[i]));
            searchAfter = lastDocSortValues;
            response.decRef();
        }
    }

    public void testSortMixedFieldTypesWithNoDocsForOneType() {
        assertAcked(
            prepareCreate("index_long").setMapping("foo", "type=long"),
            prepareCreate("index_other").setMapping("bar", "type=keyword"),
            prepareCreate("index_int").setMapping("foo", "type=integer")
        );

        prepareIndex("index_long").setId("1").setSource("foo", "123").get();
        prepareIndex("index_long").setId("2").setSource("foo", "124").get();
        prepareIndex("index_other").setId("1").setSource("bar", "124").get();
        refresh();

        assertNoFailures(
            prepareSearch("index_long", "index_int", "index_other").addSort(new FieldSortBuilder("foo").unmappedType("boolean")).setSize(10)
        );
    }
}
