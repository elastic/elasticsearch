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

package org.elasticsearch.search.sort;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.fieldValueFactorFunction;
import static org.elasticsearch.script.MockScriptPlugin.NAME;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFirstHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSecondHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasId;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class FieldSortIT extends ESIntegTestCase {
    public static class CustomScriptPlugin extends MockScriptPlugin {
        @Override
        @SuppressWarnings("unchecked")
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();
            scripts.put("doc['number'].value", vars -> sortDoubleScript(vars));
            scripts.put("doc['keyword'].value", vars -> sortStringScript(vars));
            return scripts;
        }

        @SuppressWarnings("unchecked")
        static Double sortDoubleScript(Map<String, Object> vars) {
            Map<?, ?> doc = (Map) vars.get("doc");
            Double index = ((Number) ((ScriptDocValues<?>) doc.get("number")).getValues().get(0)).doubleValue();
            return index;
        }

        @SuppressWarnings("unchecked")
        static String sortStringScript(Map<String, Object> vars) {
            Map<?, ?> doc = (Map) vars.get("doc");
            String value = ((String) ((ScriptDocValues<?>) doc.get("keyword")).getValues().get(0));
            return value;
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
                assertAcked(prepareCreate("test_" + i).addAlias(new Alias("test")).addMapping("foo", "entry", "type=long"));
            } else {
                assertAcked(prepareCreate("test_" + i).addAlias(new Alias("test")));
            }
            if (i > 0) {
                client().prepareIndex("test_" + i, "foo", "" + i).setSource("{\"entry\": " + i + "}", XContentType.JSON).get();
            }
        }
        refresh();
        // sort DESC
        SearchResponse searchResponse = client().prepareSearch()
                .addSort(new FieldSortBuilder("entry").order(SortOrder.DESC).unmappedType(useMapping ? null : "long"))
                .setSize(10).get();
        logClusterState();
        assertSearchResponse(searchResponse);

        for (int j = 1; j < searchResponse.getHits().getHits().length; j++) {
            Number current = (Number) searchResponse.getHits().getHits()[j].getSourceAsMap().get("entry");
            Number previous = (Number) searchResponse.getHits().getHits()[j-1].getSourceAsMap().get("entry");
            assertThat(searchResponse.toString(), current.intValue(), lessThan(previous.intValue()));
        }

        // sort ASC
        searchResponse = client().prepareSearch()
                .addSort(new FieldSortBuilder("entry").order(SortOrder.ASC).unmappedType(useMapping ? null : "long"))
                .setSize(10).get();
        logClusterState();
        assertSearchResponse(searchResponse);

        for (int j = 1; j < searchResponse.getHits().getHits().length; j++) {
            Number current = (Number) searchResponse.getHits().getHits()[j].getSourceAsMap().get("entry");
            Number previous = (Number) searchResponse.getHits().getHits()[j-1].getSourceAsMap().get("entry");
            assertThat(searchResponse.toString(), current.intValue(), greaterThan(previous.intValue()));
        }
    }

    public void testIssue6614() throws ExecutionException, InterruptedException {
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
                    client().prepareIndex(indexId, "type").setSource(
                            "foo", "bar", "timeUpdated", "2014/07/" +
                                    String.format(Locale.ROOT, "%02d", i+1)+
                                    " " +
                                    String.format(Locale.ROOT, "%02d", j+1) +
                                    ":00:00"));
          }
            indexRandom(true, builders);
            docs += builders.size();
            builders.clear();
        }
        SearchResponse allDocsResponse = client().prepareSearch().setQuery(
                QueryBuilders.boolQuery().must(QueryBuilders.termQuery("foo", "bar")).must(
                        QueryBuilders.rangeQuery("timeUpdated").gte("2014/0" + randomIntBetween(1, 7) + "/01")))
                .addSort(new FieldSortBuilder("timeUpdated").order(SortOrder.ASC).unmappedType("date"))
                .setSize(docs).get();
        assertSearchResponse(allDocsResponse);

        final int numiters = randomIntBetween(1, 20);
        for (int i = 0; i < numiters; i++) {
            SearchResponse searchResponse = client().prepareSearch().setQuery(
                    QueryBuilders.boolQuery().must(QueryBuilders.termQuery("foo", "bar")).must(
                            QueryBuilders.rangeQuery("timeUpdated").gte(
                                    "2014/" + String.format(Locale.ROOT, "%02d", randomIntBetween(1, 7)) + "/01")))
                    .addSort(new FieldSortBuilder("timeUpdated").order(SortOrder.ASC).unmappedType("date"))
                    .setSize(scaledRandomIntBetween(1, docs)).get();
            assertSearchResponse(searchResponse);
            for (int j = 0; j < searchResponse.getHits().getHits().length; j++) {
                assertThat(searchResponse.toString() +
                        "\n vs. \n" +
                        allDocsResponse.toString(),
                        searchResponse.getHits().getHits()[j].getId(),
                        equalTo(allDocsResponse.getHits().getHits()[j].getId()));
            }
        }

    }

    public void testTrackScores() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                .addMapping("type1", "svalue", "type=keyword").get());
        ensureGreen();
        index("test", "type1", jsonBuilder().startObject()
                .field("id", "1")
                .field("svalue", "aaa")
                .field("ivalue", 100)
                .field("dvalue", 0.1)
                .endObject());
        index("test", "type1", jsonBuilder().startObject()
                .field("id", "2")
                .field("svalue", "bbb")
                .field("ivalue", 200)
                .field("dvalue", 0.2)
                .endObject());
        refresh();

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort("svalue", SortOrder.ASC)
                .execute().actionGet();

        assertThat(searchResponse.getHits().getMaxScore(), equalTo(Float.NaN));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.getScore(), equalTo(Float.NaN));
        }

        // now check with score tracking
        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort("svalue", SortOrder.ASC)
                .setTrackScores(true)
                .execute().actionGet();

        assertThat(searchResponse.getHits().getMaxScore(), not(equalTo(Float.NaN)));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.getScore(), not(equalTo(Float.NaN)));
        }
    }

    public void testRandomSorting() throws IOException, InterruptedException, ExecutionException {
        Random random = random();
        assertAcked(prepareCreate("test")
                .addMapping("type",
                        XContentFactory.jsonBuilder()
                                .startObject()
                                .startObject("type")
                                .startObject("properties")
                                .startObject("sparse_bytes")
                                .field("type", "keyword")
                                .endObject()
                                .startObject("dense_bytes")
                                .field("type", "keyword")
                                .endObject()
                                .endObject()
                                .endObject()
                                .endObject()));
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
            builders[i] = client().prepareIndex("test", "type", docId).setSource(src);
        }
        indexRandom(true, builders);
        {
            int size = between(1, denseBytes.size());
            SearchResponse searchResponse = client().prepareSearch("test").setQuery(matchAllQuery()).setSize(size)
                    .addSort("dense_bytes", SortOrder.ASC).execute().actionGet();
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits(), equalTo((long) numDocs));
            assertThat(searchResponse.getHits().getHits().length, equalTo(size));
            Set<Entry<BytesRef, String>> entrySet = denseBytes.entrySet();
            Iterator<Entry<BytesRef, String>> iterator = entrySet.iterator();
            for (int i = 0; i < size; i++) {
                assertThat(iterator.hasNext(), equalTo(true));
                Entry<BytesRef, String> next = iterator.next();
                assertThat("pos: " + i, searchResponse.getHits().getAt(i).getId(), equalTo(next.getValue()));
                assertThat(searchResponse.getHits().getAt(i).getSortValues()[0].toString(), equalTo(next.getKey().utf8ToString()));
            }
        }
        if (!sparseBytes.isEmpty()) {
            int size = between(1, sparseBytes.size());
            SearchResponse searchResponse = client().prepareSearch().setQuery(matchAllQuery())
                    .setPostFilter(QueryBuilders.existsQuery("sparse_bytes")).setSize(size).addSort("sparse_bytes", SortOrder.ASC).execute()
                    .actionGet();
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits(), equalTo((long) sparseBytes.size()));
            assertThat(searchResponse.getHits().getHits().length, equalTo(size));
            Set<Entry<BytesRef, String>> entrySet = sparseBytes.entrySet();
            Iterator<Entry<BytesRef, String>> iterator = entrySet.iterator();
            for (int i = 0; i < size; i++) {
                assertThat(iterator.hasNext(), equalTo(true));
                Entry<BytesRef, String> next = iterator.next();
                assertThat(searchResponse.getHits().getAt(i).getId(), equalTo(next.getValue()));
                assertThat(searchResponse.getHits().getAt(i).getSortValues()[0].toString(), equalTo(next.getKey().utf8ToString()));
            }
        }
    }

    public void test3078() {
        assertAcked(client().admin().indices().prepareCreate("test")
                .addMapping("type", "field", "type=keyword").get());
        ensureGreen();

        for (int i = 1; i < 101; i++) {
            client().prepareIndex("test", "type", Integer.toString(i)).setSource("field", Integer.toString(i)).execute().actionGet();
        }
        refresh();
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(matchAllQuery())
                .addSort(SortBuilders.fieldSort("field").order(SortOrder.ASC)).execute().actionGet();
        assertThat(searchResponse.getHits().getAt(0).getSortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getSortValues()[0].toString(), equalTo("10"));
        assertThat(searchResponse.getHits().getAt(2).getSortValues()[0].toString(), equalTo("100"));

        // reindex and refresh
        client().prepareIndex("test", "type", Integer.toString(1)).setSource("field", Integer.toString(1)).execute().actionGet();
        refresh();

        searchResponse = client().prepareSearch("test").setQuery(matchAllQuery())
                .addSort(SortBuilders.fieldSort("field").order(SortOrder.ASC)).execute().actionGet();
        assertThat(searchResponse.getHits().getAt(0).getSortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getSortValues()[0].toString(), equalTo("10"));
        assertThat(searchResponse.getHits().getAt(2).getSortValues()[0].toString(), equalTo("100"));

        // reindex - no refresh
        client().prepareIndex("test", "type", Integer.toString(1)).setSource("field", Integer.toString(1)).execute().actionGet();

        searchResponse = client().prepareSearch("test").setQuery(matchAllQuery())
                .addSort(SortBuilders.fieldSort("field").order(SortOrder.ASC)).execute().actionGet();
        assertThat(searchResponse.getHits().getAt(0).getSortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getSortValues()[0].toString(), equalTo("10"));
        assertThat(searchResponse.getHits().getAt(2).getSortValues()[0].toString(), equalTo("100"));

        // force merge
        forceMerge();
        refresh();

        client().prepareIndex("test", "type", Integer.toString(1)).setSource("field", Integer.toString(1)).execute().actionGet();
        searchResponse = client().prepareSearch("test").setQuery(matchAllQuery())
                .addSort(SortBuilders.fieldSort("field").order(SortOrder.ASC)).execute().actionGet();
        assertThat(searchResponse.getHits().getAt(0).getSortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getSortValues()[0].toString(), equalTo("10"));
        assertThat(searchResponse.getHits().getAt(2).getSortValues()[0].toString(), equalTo("100"));

        refresh();
        searchResponse = client().prepareSearch("test").setQuery(matchAllQuery())
                .addSort(SortBuilders.fieldSort("field").order(SortOrder.ASC)).execute().actionGet();
        assertThat(searchResponse.getHits().getAt(0).getSortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getSortValues()[0].toString(), equalTo("10"));
        assertThat(searchResponse.getHits().getAt(2).getSortValues()[0].toString(), equalTo("100"));
    }

    public void testScoreSortDirection() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "type", "1").setSource("field", 2).execute().actionGet();
        client().prepareIndex("test", "type", "2").setSource("field", 1).execute().actionGet();
        client().prepareIndex("test", "type", "3").setSource("field", 0).execute().actionGet();

        refresh();

        SearchResponse searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        QueryBuilders.functionScoreQuery(matchAllQuery(), ScoreFunctionBuilders.fieldValueFactorFunction("field")))
                .execute().actionGet();
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getScore(), Matchers.lessThan(searchResponse.getHits().getAt(0).getScore()));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(2).getScore(), Matchers.lessThan(searchResponse.getHits().getAt(1).getScore()));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));

        searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        QueryBuilders.functionScoreQuery(matchAllQuery(), ScoreFunctionBuilders.fieldValueFactorFunction("field")))
                .addSort("_score", SortOrder.DESC).execute().actionGet();
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getScore(), Matchers.lessThan(searchResponse.getHits().getAt(0).getScore()));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(2).getScore(), Matchers.lessThan(searchResponse.getHits().getAt(1).getScore()));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));

        searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        QueryBuilders.functionScoreQuery(matchAllQuery(), ScoreFunctionBuilders.fieldValueFactorFunction("field")))
                .addSort("_score", SortOrder.DESC).execute().actionGet();
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
    }

    public void testScoreSortDirectionWithFunctionScore() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "type", "1").setSource("field", 2).execute().actionGet();
        client().prepareIndex("test", "type", "2").setSource("field", 1).execute().actionGet();
        client().prepareIndex("test", "type", "3").setSource("field", 0).execute().actionGet();

        refresh();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(functionScoreQuery(matchAllQuery(), fieldValueFactorFunction("field"))).execute().actionGet();
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getScore(), Matchers.lessThan(searchResponse.getHits().getAt(0).getScore()));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(2).getScore(), Matchers.lessThan(searchResponse.getHits().getAt(1).getScore()));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));

        searchResponse = client().prepareSearch("test")
                .setQuery(functionScoreQuery(matchAllQuery(), fieldValueFactorFunction("field")))
                .addSort("_score", SortOrder.DESC).execute().actionGet();
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getScore(), Matchers.lessThan(searchResponse.getHits().getAt(0).getScore()));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(2).getScore(), Matchers.lessThan(searchResponse.getHits().getAt(1).getScore()));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));

        searchResponse = client().prepareSearch("test")
                .setQuery(functionScoreQuery(matchAllQuery(), fieldValueFactorFunction("field")))
                .addSort("_score", SortOrder.DESC).execute().actionGet();
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
    }

    public void testIssue2986() {
        assertAcked(client().admin().indices().prepareCreate("test")
                .addMapping("post", "field1", "type=keyword").get());

        client().prepareIndex("test", "post", "1").setSource("{\"field1\":\"value1\"}", XContentType.JSON).execute().actionGet();
        client().prepareIndex("test", "post", "2").setSource("{\"field1\":\"value2\"}", XContentType.JSON).execute().actionGet();
        client().prepareIndex("test", "post", "3").setSource("{\"field1\":\"value3\"}", XContentType.JSON).execute().actionGet();
        refresh();
        SearchResponse result = client().prepareSearch("test").setQuery(matchAllQuery()).setTrackScores(true)
                .addSort("field1", SortOrder.ASC).execute().actionGet();

        for (SearchHit hit : result.getHits()) {
            assertFalse(Float.isNaN(hit.getScore()));
        }
    }

    public void testIssue2991() {
        for (int i = 1; i < 4; i++) {
            try {
                client().admin().indices().prepareDelete("test").execute().actionGet();
            } catch (Exception e) {
                // ignore
            }
            assertAcked(client().admin().indices().prepareCreate("test")
                    .addMapping("type", "tag", "type=keyword").get());
            ensureGreen();
            client().prepareIndex("test", "type", "1").setSource("tag", "alpha").execute().actionGet();
            refresh();

            client().prepareIndex("test", "type", "3").setSource("tag", "gamma").execute().actionGet();
            refresh();

            client().prepareIndex("test", "type", "4").setSource("tag", "delta").execute().actionGet();

            refresh();
            client().prepareIndex("test", "type", "2").setSource("tag", "beta").execute().actionGet();

            refresh();
            SearchResponse resp = client().prepareSearch("test").setSize(2).setQuery(matchAllQuery())
                    .addSort(SortBuilders.fieldSort("tag").order(SortOrder.ASC)).execute().actionGet();
            assertHitCount(resp, 4);
            assertThat(resp.getHits().getHits().length, equalTo(2));
            assertFirstHit(resp, hasId("1"));
            assertSecondHit(resp, hasId("2"));

            resp = client().prepareSearch("test").setSize(2).setQuery(matchAllQuery())
                    .addSort(SortBuilders.fieldSort("tag").order(SortOrder.DESC)).execute().actionGet();
            assertHitCount(resp, 4);
            assertThat(resp.getHits().getHits().length, equalTo(2));
            assertFirstHit(resp, hasId("3"));
            assertSecondHit(resp, hasId("4"));
        }
    }

    public void testSimpleSorts() throws Exception {
        Random random = random();
        assertAcked(prepareCreate("test")
.addMapping("type1",
                XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("str_value")
                        .field("type", "keyword").endObject().startObject("boolean_value").field("type", "boolean").endObject()
                        .startObject("byte_value").field("type", "byte").endObject().startObject("short_value").field("type", "short")
                        .endObject().startObject("integer_value").field("type", "integer").endObject().startObject("long_value")
                        .field("type", "long").endObject().startObject("float_value").field("type", "float").endObject()
                        .startObject("double_value").field("type", "double").endObject().endObject().endObject().endObject()));
        ensureGreen();
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            IndexRequestBuilder builder = client().prepareIndex("test", "type1", Integer.toString(i)).setSource(jsonBuilder().startObject()
                    .field("str_value", new String(new char[]{(char) (97 + i), (char) (97 + i)}))
                    .field("boolean_value", true)
                    .field("byte_value", i)
                    .field("short_value", i)
                    .field("integer_value", i)
                    .field("long_value", i)
                    .field("float_value", 0.1 * i)
                    .field("double_value", 0.1 * i)
                    .endObject());
            builders.add(builder);
        }
        Collections.shuffle(builders, random);
        for (IndexRequestBuilder builder : builders) {
            builder.execute().actionGet();
            if (random.nextBoolean()) {
                if (random.nextInt(5) != 0) {
                    refresh();
                } else {
                    client().admin().indices().prepareFlush().execute().actionGet();
                }
            }

        }
        refresh();

        // STRING
        int size = 1 + random.nextInt(10);

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(size)
                .addSort("str_value", SortOrder.ASC)
                .execute().actionGet();
        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().getHits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).getId(), equalTo(Integer.toString(i)));
            assertThat(searchResponse.getHits().getAt(i).getSortValues()[0].toString(),
                    equalTo(new String(new char[] { (char) (97 + i), (char) (97 + i) })));
        }
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(size)
                .addSort("str_value", SortOrder.DESC)
                .execute().actionGet();

        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().getHits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).getId(), equalTo(Integer.toString(9 - i)));
            assertThat(searchResponse.getHits().getAt(i).getSortValues()[0].toString(),
                    equalTo(new String(new char[] { (char) (97 + (9 - i)), (char) (97 + (9 - i)) })));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));


        // BYTE
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("byte_value", SortOrder.ASC).execute()
                .actionGet();

        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().getHits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).getId(), equalTo(Integer.toString(i)));
            assertThat(((Number) searchResponse.getHits().getAt(i).getSortValues()[0]).byteValue(), equalTo((byte) i));
        }
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("byte_value", SortOrder.DESC).execute()
                .actionGet();

        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().getHits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).getId(), equalTo(Integer.toString(9 - i)));
            assertThat(((Number) searchResponse.getHits().getAt(i).getSortValues()[0]).byteValue(), equalTo((byte) (9 - i)));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));

        // SHORT
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("short_value", SortOrder.ASC).execute()
                .actionGet();

        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().getHits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).getId(), equalTo(Integer.toString(i)));
            assertThat(((Number) searchResponse.getHits().getAt(i).getSortValues()[0]).shortValue(), equalTo((short) i));
        }
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("short_value", SortOrder.DESC).execute()
                .actionGet();

        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().getHits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).getId(), equalTo(Integer.toString(9 - i)));
            assertThat(((Number) searchResponse.getHits().getAt(i).getSortValues()[0]).shortValue(), equalTo((short) (9 - i)));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));

        // INTEGER
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("integer_value", SortOrder.ASC).execute()
                .actionGet();

        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().getHits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).getId(), equalTo(Integer.toString(i)));
            assertThat(((Number) searchResponse.getHits().getAt(i).getSortValues()[0]).intValue(), equalTo(i));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("integer_value", SortOrder.DESC)
                .execute().actionGet();

        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().getHits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).getId(), equalTo(Integer.toString(9 - i)));
            assertThat(((Number) searchResponse.getHits().getAt(i).getSortValues()[0]).intValue(), equalTo((9 - i)));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));

        // LONG
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("long_value", SortOrder.ASC).execute()
                .actionGet();

        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().getHits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).getId(), equalTo(Integer.toString(i)));
            assertThat(((Number) searchResponse.getHits().getAt(i).getSortValues()[0]).longValue(), equalTo((long) i));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("long_value", SortOrder.DESC).execute()
                .actionGet();
        assertHitCount(searchResponse, 10L);
        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().getHits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).getId(), equalTo(Integer.toString(9 - i)));
            assertThat(((Number) searchResponse.getHits().getAt(i).getSortValues()[0]).longValue(), equalTo((long) (9 - i)));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));

        // FLOAT
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("float_value", SortOrder.ASC).execute()
                .actionGet();

        assertHitCount(searchResponse, 10L);
        assertThat(searchResponse.getHits().getHits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).getId(), equalTo(Integer.toString(i)));
            assertThat(((Number) searchResponse.getHits().getAt(i).getSortValues()[0]).doubleValue(), closeTo(0.1d * i, 0.000001d));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("float_value", SortOrder.DESC).execute()
                .actionGet();

        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().getHits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).getId(), equalTo(Integer.toString(9 - i)));
            assertThat(((Number) searchResponse.getHits().getAt(i).getSortValues()[0]).doubleValue(), closeTo(0.1d * (9 - i), 0.000001d));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));

        // DOUBLE
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("double_value", SortOrder.ASC).execute()
                .actionGet();

        assertHitCount(searchResponse, 10L);
        assertThat(searchResponse.getHits().getHits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).getId(), equalTo(Integer.toString(i)));
            assertThat(((Number) searchResponse.getHits().getAt(i).getSortValues()[0]).doubleValue(), closeTo(0.1d * i, 0.000001d));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("double_value", SortOrder.DESC).execute()
                .actionGet();

        assertHitCount(searchResponse, 10L);
        assertThat(searchResponse.getHits().getHits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).getId(), equalTo(Integer.toString(9 - i)));
            assertThat(((Number) searchResponse.getHits().getAt(i).getSortValues()[0]).doubleValue(), closeTo(0.1d * (9 - i), 0.000001d));
        }

        assertNoFailures(searchResponse);
    }

    public void testSortMissingNumbers() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type1",
                XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("type1")
                        .startObject("properties")
                            .startObject("i_value")
                                .field("type", "integer")
                            .endObject()
                            .startObject("d_value")
                                .field("type", "float")
                            .endObject()
                        .endObject()
                        .endObject()
                        .endObject()));
        ensureGreen();
        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("id", "1")
                .field("i_value", -1)
                .field("d_value", -1.1)
                .endObject()).execute().actionGet();

        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
                .field("id", "2")
                .endObject()).execute().actionGet();

        client().prepareIndex("test", "type1", "3").setSource(jsonBuilder().startObject()
                .field("id", "1")
                .field("i_value", 2)
                .field("d_value", 2.2)
                .endObject()).execute().actionGet();

        flush();
        refresh();

        logger.info("--> sort with no missing (same as missing _last)");
        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(SortBuilders.fieldSort("i_value").order(SortOrder.ASC))
                .execute().actionGet();
        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3L));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("2"));

        logger.info("--> sort with missing _last");
        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(SortBuilders.fieldSort("i_value").order(SortOrder.ASC).missing("_last"))
                .execute().actionGet();
        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3L));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("2"));

        logger.info("--> sort with missing _first");
        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(SortBuilders.fieldSort("i_value").order(SortOrder.ASC).missing("_first"))
                .execute().actionGet();
        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3L));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));
    }

    public void testSortMissingStrings() throws IOException {
        assertAcked(prepareCreate("test").addMapping("type1",
                XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("type1")
                        .startObject("properties")
                        .startObject("value")
                        .field("type", "keyword")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()));
        ensureGreen();
        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("id", "1")
                .field("value", "a")
                .endObject()).execute().actionGet();

        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
                .field("id", "2")
                .endObject()).execute().actionGet();

        client().prepareIndex("test", "type1", "3").setSource(jsonBuilder().startObject()
                .field("id", "1")
                .field("value", "c")
                .endObject()).execute().actionGet();

        flush();
        refresh();

        // TODO: WTF?
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            throw new RuntimeException();
        }

        logger.info("--> sort with no missing (same as missing _last)");
        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(SortBuilders.fieldSort("value").order(SortOrder.ASC))
                .execute().actionGet();
        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3L));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("2"));

        logger.info("--> sort with missing _last");
        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(SortBuilders.fieldSort("value").order(SortOrder.ASC).missing("_last"))
                .execute().actionGet();
        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3L));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("2"));

        logger.info("--> sort with missing _first");
        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(SortBuilders.fieldSort("value").order(SortOrder.ASC).missing("_first"))
                .execute().actionGet();
        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3L));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));

        logger.info("--> sort with missing b");
        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(SortBuilders.fieldSort("value").order(SortOrder.ASC).missing("b"))
                .execute().actionGet();
        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3L));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));
    }

    public void testIgnoreUnmapped() throws Exception {
        createIndex("test");

        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("id", "1")
                .field("i_value", -1)
                .field("d_value", -1.1)
                .endObject()).execute().actionGet();

        logger.info("--> sort with an unmapped field, verify it fails");
        try {
            SearchResponse result = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .addSort(SortBuilders.fieldSort("kkk"))
                    .execute().actionGet();
            assertThat("Expected exception but returned with", result, nullValue());
        } catch (SearchPhaseExecutionException e) {
            //we check that it's a parse failure rather than a different shard failure
            for (ShardSearchFailure shardSearchFailure : e.shardFailures()) {
                assertThat(shardSearchFailure.toString(), containsString("[No mapping found for [kkk] in order to sort on]"));
            }
        }

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(SortBuilders.fieldSort("kkk").unmappedType("keyword"))
                .execute().actionGet();
        assertNoFailures(searchResponse);
    }

    public void testSortMVField() throws Exception {
        assertAcked(prepareCreate("test")
                        .addMapping("type1",
                XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("long_values")
                        .field("type", "long").endObject().startObject("int_values").field("type", "integer").endObject()
                        .startObject("short_values").field("type", "short").endObject().startObject("byte_values")
                        .field("type", "byte").endObject().startObject("float_values").field("type", "float").endObject()
                        .startObject("double_values").field("type", "double").endObject().startObject("string_values")
                        .field("type", "keyword").endObject().endObject().endObject()
                        .endObject()));
        ensureGreen();

        client().prepareIndex("test", "type1", Integer.toString(1)).setSource(jsonBuilder().startObject()
                .array("long_values", 1L, 5L, 10L, 8L)
                .array("int_values", 1, 5, 10, 8)
                .array("short_values", 1, 5, 10, 8)
                .array("byte_values", 1, 5, 10, 8)
                .array("float_values", 1f, 5f, 10f, 8f)
                .array("double_values", 1d, 5d, 10d, 8d)
                .array("string_values", "01", "05", "10", "08")
                .endObject()).execute().actionGet();
        client().prepareIndex("test", "type1", Integer.toString(2)).setSource(jsonBuilder().startObject()
                .array("long_values", 11L, 15L, 20L, 7L)
                .array("int_values", 11, 15, 20, 7)
                .array("short_values", 11, 15, 20, 7)
                .array("byte_values", 11, 15, 20, 7)
                .array("float_values", 11f, 15f, 20f, 7f)
                .array("double_values", 11d, 15d, 20d, 7d)
                .array("string_values", "11", "15", "20", "07")
                .endObject()).execute().actionGet();
        client().prepareIndex("test", "type1", Integer.toString(3)).setSource(jsonBuilder().startObject()
                .array("long_values", 2L, 1L, 3L, -4L)
                .array("int_values", 2, 1, 3, -4)
                .array("short_values", 2, 1, 3, -4)
                .array("byte_values", 2, 1, 3, -4)
                .array("float_values", 2f, 1f, 3f, -4f)
                .array("double_values", 2d, 1d, 3d, -4d)
                .array("string_values", "02", "01", "03", "!4")
                .endObject()).execute().actionGet();

        refresh();

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("long_values", SortOrder.ASC)
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).longValue(), equalTo(-4L));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).longValue(), equalTo(1L));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).longValue(), equalTo(7L));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("long_values", SortOrder.DESC)
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).longValue(), equalTo(20L));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).longValue(), equalTo(10L));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).longValue(), equalTo(3L));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort(SortBuilders.fieldSort("long_values").order(SortOrder.DESC).sortMode(SortMode.SUM))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).longValue(), equalTo(53L));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).longValue(), equalTo(24L));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).longValue(), equalTo(2L));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort(SortBuilders.fieldSort("long_values").order(SortOrder.DESC).sortMode(SortMode.AVG))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).longValue(), equalTo(13L));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).longValue(), equalTo(6L));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).longValue(), equalTo(1L));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort(SortBuilders.fieldSort("long_values").order(SortOrder.DESC).sortMode(SortMode.MEDIAN))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).longValue(), equalTo(13L));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).longValue(), equalTo(7L));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).longValue(), equalTo(2L));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("int_values", SortOrder.ASC)
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).intValue(), equalTo(-4));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).intValue(), equalTo(1));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).intValue(), equalTo(7));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("int_values", SortOrder.DESC)
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).intValue(), equalTo(20));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).intValue(), equalTo(10));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).intValue(), equalTo(3));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("short_values", SortOrder.ASC)
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).intValue(), equalTo(-4));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).intValue(), equalTo(1));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).intValue(), equalTo(7));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("short_values", SortOrder.DESC)
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).intValue(), equalTo(20));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).intValue(), equalTo(10));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).intValue(), equalTo(3));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("byte_values", SortOrder.ASC)
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).intValue(), equalTo(-4));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).intValue(), equalTo(1));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).intValue(), equalTo(7));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("byte_values", SortOrder.DESC)
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).intValue(), equalTo(20));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).intValue(), equalTo(10));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).intValue(), equalTo(3));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("float_values", SortOrder.ASC)
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).floatValue(), equalTo(-4f));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).floatValue(), equalTo(1f));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).floatValue(), equalTo(7f));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("float_values", SortOrder.DESC)
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).floatValue(), equalTo(20f));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).floatValue(), equalTo(10f));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).floatValue(), equalTo(3f));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("double_values", SortOrder.ASC)
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).doubleValue(), equalTo(-4d));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).doubleValue(), equalTo(1d));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).doubleValue(), equalTo(7d));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("double_values", SortOrder.DESC)
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).doubleValue(), equalTo(20d));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).doubleValue(), equalTo(10d));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).doubleValue(), equalTo(3d));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("string_values", SortOrder.ASC)
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(3)));
        assertThat(searchResponse.getHits().getAt(0).getSortValues()[0], equalTo("!4"));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(searchResponse.getHits().getAt(1).getSortValues()[0], equalTo("01"));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo(Integer.toString(2)));
        assertThat(searchResponse.getHits().getAt(2).getSortValues()[0], equalTo("07"));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("string_values", SortOrder.DESC)
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(2)));
        assertThat(searchResponse.getHits().getAt(0).getSortValues()[0], equalTo("20"));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(searchResponse.getHits().getAt(1).getSortValues()[0], equalTo("10"));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo(Integer.toString(3)));
        assertThat(searchResponse.getHits().getAt(2).getSortValues()[0], equalTo("03"));
    }

    public void testSortOnRareField() throws IOException {
        assertAcked(prepareCreate("test").addMapping("type1",
                XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("string_values")
                        .field("type", "keyword").endObject().endObject().endObject().endObject()));
        ensureGreen();
        client().prepareIndex("test", "type1", Integer.toString(1)).setSource(jsonBuilder().startObject()
                .array("string_values", "01", "05", "10", "08")
                .endObject()).execute().actionGet();


        refresh();
        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(3)
                .addSort("string_values", SortOrder.DESC)
                .execute().actionGet();

        assertThat(searchResponse.getHits().getHits().length, equalTo(1));


        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(1)));
        assertThat(searchResponse.getHits().getAt(0).getSortValues()[0], equalTo("10"));

        client().prepareIndex("test", "type1", Integer.toString(2)).setSource(jsonBuilder().startObject()
                .array("string_values", "11", "15", "20", "07")
                .endObject()).execute().actionGet();
        for (int i = 0; i < 15; i++) {
            client().prepareIndex("test", "type1", Integer.toString(300 + i)).setSource(jsonBuilder().startObject()
                    .array("some_other_field", "foobar")
                    .endObject()).execute().actionGet();
        }
        refresh();

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(2)
                .addSort("string_values", SortOrder.DESC)
                .execute().actionGet();

        assertThat(searchResponse.getHits().getHits().length, equalTo(2));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(2)));
        assertThat(searchResponse.getHits().getAt(0).getSortValues()[0], equalTo("20"));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(searchResponse.getHits().getAt(1).getSortValues()[0], equalTo("10"));


        client().prepareIndex("test", "type1", Integer.toString(3)).setSource(jsonBuilder().startObject()
                .array("string_values", "02", "01", "03", "!4")
                .endObject()).execute().actionGet();
        for (int i = 0; i < 15; i++) {
            client().prepareIndex("test", "type1", Integer.toString(300 + i)).setSource(jsonBuilder().startObject()
                    .array("some_other_field", "foobar")
                    .endObject()).execute().actionGet();
        }
        refresh();

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(3)
                .addSort("string_values", SortOrder.DESC)
                .execute().actionGet();

        assertThat(searchResponse.getHits().getHits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(2)));
        assertThat(searchResponse.getHits().getAt(0).getSortValues()[0], equalTo("20"));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(searchResponse.getHits().getAt(1).getSortValues()[0], equalTo("10"));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo(Integer.toString(3)));
        assertThat(searchResponse.getHits().getAt(2).getSortValues()[0], equalTo("03"));

        for (int i = 0; i < 15; i++) {
            client().prepareIndex("test", "type1", Integer.toString(300 + i)).setSource(jsonBuilder().startObject()
                    .array("some_other_field", "foobar")
                    .endObject()).execute().actionGet();
            refresh();
        }

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(3)
                .addSort("string_values", SortOrder.DESC)
                .execute().actionGet();

        assertThat(searchResponse.getHits().getHits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(2)));
        assertThat(searchResponse.getHits().getAt(0).getSortValues()[0], equalTo("20"));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(searchResponse.getHits().getAt(1).getSortValues()[0], equalTo("10"));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo(Integer.toString(3)));
        assertThat(searchResponse.getHits().getAt(2).getSortValues()[0], equalTo("03"));
    }

    public void testSortMetaField() throws Exception {
        createIndex("test");
        ensureGreen();
        final int numDocs = randomIntBetween(10, 20);
        IndexRequestBuilder[] indexReqs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; ++i) {
            indexReqs[i] = client().prepareIndex("test", "type", Integer.toString(i))
                    .setSource();
        }
        indexRandom(true, indexReqs);

        SortOrder order = randomFrom(SortOrder.values());
        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(randomIntBetween(1, numDocs + 5))
                .addSort("_uid", order)
                .execute().actionGet();
        assertNoFailures(searchResponse);
        SearchHit[] hits = searchResponse.getHits().getHits();
        BytesRef previous = order == SortOrder.ASC ? new BytesRef() : UnicodeUtil.BIG_TERM;
        for (int i = 0; i < hits.length; ++i) {
            String uidString = Uid.createUid(hits[i].getType(), hits[i].getId());
            final BytesRef uid = new BytesRef(uidString);
            assertEquals(uidString, hits[i].getSortValues()[0]);
            assertThat(previous, order == SortOrder.ASC ? lessThan(uid) : greaterThan(uid));
            previous = uid;
        }
    }

    /**
     * Test case for issue 6150: https://github.com/elastic/elasticsearch/issues/6150
     */
    public void testNestedSort() throws IOException, InterruptedException, ExecutionException {
        assertAcked(prepareCreate("test")
                .addMapping("type",
                        XContentFactory.jsonBuilder()
                                .startObject()
                                    .startObject("type")
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
                                                .endObject()
                                            .endObject()
                                        .endObject()
                                    .endObject()
                                .endObject()));
        ensureGreen();

        client().prepareIndex("test", "type", "1").setSource(jsonBuilder().startObject()
                .startObject("nested")
                    .field("foo", "bar bar")
                .endObject()
                .endObject()).execute().actionGet();
        refresh();

        // We sort on nested field
        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(SortBuilders.fieldSort("nested.foo").setNestedPath("nested").order(SortOrder.DESC))
                .execute().actionGet();
        assertNoFailures(searchResponse);
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (int i = 0; i < hits.length; ++i) {
            assertThat(hits[i].getSortValues().length, is(1));
            assertThat(hits[i].getSortValues()[0], is("bar"));
        }


        // We sort on nested sub field
        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(SortBuilders.fieldSort("nested.foo.sub").setNestedPath("nested").order(SortOrder.DESC))
                .execute().actionGet();
        assertNoFailures(searchResponse);
        hits = searchResponse.getHits().getHits();
        for (int i = 0; i < hits.length; ++i) {
            assertThat(hits[i].getSortValues().length, is(1));
            assertThat(hits[i].getSortValues()[0], is("bar bar"));
        }
    }

    public void testSortDuelBetweenSingleShardAndMultiShardIndex() throws Exception {
        String sortField = "sortField";
        assertAcked(prepareCreate("test1")
                .setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, between(2, maximumNumberOfShards())))
                .addMapping("type", sortField, "type=long").get());
        assertAcked(prepareCreate("test2")
                .setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1))
                .addMapping("type", sortField, "type=long").get());

        for (String index : new String[]{"test1", "test2"}) {
            List<IndexRequestBuilder> docs = new ArrayList<>();
            for (int i = 0; i < 256; i++) {
                docs.add(client().prepareIndex(index, "type", Integer.toString(i)).setSource(sortField, i));
            }
            indexRandom(true, docs);
        }

        ensureSearchable("test1", "test2");
        SortOrder order = randomBoolean() ? SortOrder.ASC : SortOrder.DESC;
        int from = between(0, 256);
        int size = between(0, 256);
        SearchResponse multiShardResponse = client().prepareSearch("test1").setFrom(from).setSize(size).addSort(sortField, order).get();
        assertNoFailures(multiShardResponse);
        SearchResponse singleShardResponse = client().prepareSearch("test2").setFrom(from).setSize(size).addSort(sortField, order).get();
        assertNoFailures(singleShardResponse);

        assertThat(multiShardResponse.getHits().getTotalHits(), equalTo(singleShardResponse.getHits().getTotalHits()));
        assertThat(multiShardResponse.getHits().getHits().length, equalTo(singleShardResponse.getHits().getHits().length));
        for (int i = 0; i < multiShardResponse.getHits().getHits().length; i++) {
            assertThat(multiShardResponse.getHits().getAt(i).getSortValues()[0],
                    equalTo(singleShardResponse.getHits().getAt(i).getSortValues()[0]));
            assertThat(multiShardResponse.getHits().getAt(i).getId(), equalTo(singleShardResponse.getHits().getAt(i).getId()));
        }
    }

    public void testCustomFormat() throws Exception {
        // Use an ip field, which uses different internal/external
        // representations of values, to make sure values are both correctly
        // rendered and parsed (search_after)
        assertAcked(prepareCreate("test")
                .addMapping("type", "ip", "type=ip"));
        indexRandom(true,
                client().prepareIndex("test", "type", "1").setSource("ip", "192.168.1.7"),
                client().prepareIndex("test", "type", "2").setSource("ip", "2001:db8::ff00:42:8329"));

        SearchResponse response = client().prepareSearch("test")
                .addSort(SortBuilders.fieldSort("ip"))
                .get();
        assertSearchResponse(response);
        assertEquals(2, response.getHits().getTotalHits());
        assertArrayEquals(new String[] {"192.168.1.7"},
                response.getHits().getAt(0).getSortValues());
        assertArrayEquals(new String[] {"2001:db8::ff00:42:8329"},
                response.getHits().getAt(1).getSortValues());

        response = client().prepareSearch("test")
                .addSort(SortBuilders.fieldSort("ip"))
                .searchAfter(new Object[] {"192.168.1.7"})
                .get();
        assertSearchResponse(response);
        assertEquals(2, response.getHits().getTotalHits());
        assertEquals(1, response.getHits().getHits().length);
        assertArrayEquals(new String[] {"2001:db8::ff00:42:8329"},
                response.getHits().getAt(0).getSortValues());
    }

    public void testScriptFieldSort() throws Exception {
        assertAcked(prepareCreate("test")
            .addMapping("t", "keyword", "type=keyword", "number", "type=integer"));
        ensureGreen();
        final int numDocs = randomIntBetween(10, 20);
        IndexRequestBuilder[] indexReqs = new IndexRequestBuilder[numDocs];
        List<String> keywords = new ArrayList<>();
        for (int i = 0; i < numDocs; ++i) {
            indexReqs[i] = client().prepareIndex("test", "t")
                .setSource("number", i, "keyword", Integer.toString(i));
            keywords.add(Integer.toString(i));
        }
        Collections.sort(keywords);
        indexRandom(true, indexReqs);

        {
            Script script = new Script(ScriptType.INLINE, NAME, "doc['number'].value", Collections.emptyMap());
            SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(randomIntBetween(1, numDocs + 5))
                .addSort(SortBuilders.scriptSort(script, ScriptSortBuilder.ScriptSortType.NUMBER))
                .addSort(SortBuilders.scoreSort())
                .execute().actionGet();

            double expectedValue = 0;
            for (SearchHit hit : searchResponse.getHits()) {
                assertThat(hit.getSortValues().length, equalTo(2));
                assertThat(hit.getSortValues()[0], equalTo(expectedValue++));
                assertThat(hit.getSortValues()[1], equalTo(1f));
            }
        }

        {
            Script script = new Script(ScriptType.INLINE, NAME, "doc['keyword'].value", Collections.emptyMap());
            SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(randomIntBetween(1, numDocs + 5))
                .addSort(SortBuilders.scriptSort(script, ScriptSortBuilder.ScriptSortType.STRING))
                .addSort(SortBuilders.scoreSort())
                .execute().actionGet();

            int expectedValue = 0;
            for (SearchHit hit : searchResponse.getHits()) {
                assertThat(hit.getSortValues().length, equalTo(2));
                assertThat(hit.getSortValues()[0], equalTo(keywords.get(expectedValue++)));
                assertThat(hit.getSortValues()[1], equalTo(1f));
            }
        }
    }
}
