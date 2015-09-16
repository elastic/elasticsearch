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
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.text.StringAndBytesText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.scriptFunction;
import static org.elasticsearch.search.sort.SortBuilders.fieldSort;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFirstHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertOrderedSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSecondHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSortValues;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasId;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;


/**
 *
 */
public class SimpleSortIT extends ESIntegTestCase {

    @TestLogging("action.search.type:TRACE")
    @LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/elasticsearch/elasticsearch/issues/9421")
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
                client().prepareIndex("test_" + i, "foo", "" + i).setSource("{\"entry\": " + i + "}").get();
            }
        }
        ensureYellow();
        refresh();
        // sort DESC
        SearchResponse searchResponse = client().prepareSearch()
                .addSort(new FieldSortBuilder("entry").order(SortOrder.DESC).unmappedType(useMapping ? null : "long"))
                .setSize(10).get();
        logClusterState();
        assertSearchResponse(searchResponse);

        for (int j = 1; j < searchResponse.getHits().hits().length; j++) {
            Number current = (Number) searchResponse.getHits().hits()[j].getSource().get("entry");
            Number previous = (Number) searchResponse.getHits().hits()[j-1].getSource().get("entry");
            assertThat(searchResponse.toString(), current.intValue(), lessThan(previous.intValue()));
        }

        // sort ASC
        searchResponse = client().prepareSearch()
                .addSort(new FieldSortBuilder("entry").order(SortOrder.ASC).unmappedType(useMapping ? null : "long"))
                .setSize(10).get();
        logClusterState();
        assertSearchResponse(searchResponse);

        for (int j = 1; j < searchResponse.getHits().hits().length; j++) {
            Number current = (Number) searchResponse.getHits().hits()[j].getSource().get("entry");
            Number previous = (Number) searchResponse.getHits().hits()[j-1].getSource().get("entry");
            assertThat(searchResponse.toString(), current.intValue(), greaterThan(previous.intValue()));
        }
    }

    @LuceneTestCase.BadApple(bugUrl = "simon is working on this")
    public void testIssue6614() throws ExecutionException, InterruptedException {
        List<IndexRequestBuilder> builders = new ArrayList<>();
        boolean strictTimeBasedIndices = randomBoolean();
        final int numIndices = randomIntBetween(2, 25); // at most 25 days in the month
        for (int i = 0; i < numIndices; i++) {
          final String indexId = strictTimeBasedIndices ? "idx_" + i : "idx";
          if (strictTimeBasedIndices || i == 0) {
            createIndex(indexId);
          }
          final int numDocs = randomIntBetween(1, 23);  // hour of the day
          for (int j = 0; j < numDocs; j++) {
            builders.add(client().prepareIndex(indexId, "type").setSource("foo", "bar", "timeUpdated", "2014/07/" + String.format(Locale.ROOT, "%02d", i+1)+" " + String.format(Locale.ROOT, "%02d", j+1) + ":00:00"));
          }
        }
        int docs = builders.size();
        indexRandom(true, builders);
        ensureYellow();
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
                            QueryBuilders.rangeQuery("timeUpdated").gte("2014/" + String.format(Locale.ROOT, "%02d", randomIntBetween(1, 7)) + "/01")))
                    .addSort(new FieldSortBuilder("timeUpdated").order(SortOrder.ASC).unmappedType("date"))
                    .setSize(scaledRandomIntBetween(1, docs)).get();
            assertSearchResponse(searchResponse);
            for (int j = 0; j < searchResponse.getHits().hits().length; j++) {
                assertThat(searchResponse.toString() + "\n vs. \n" + allDocsResponse.toString(), searchResponse.getHits().hits()[j].getId(), equalTo(allDocsResponse.getHits().hits()[j].getId()));
            }
        }

    }

    public void testIssue6639() throws ExecutionException, InterruptedException {
        assertAcked(prepareCreate("$index")
                .addMapping("$type","{\"$type\": {\"properties\": {\"grantee\": {\"index\": \"not_analyzed\", \"term_vector\": \"with_positions_offsets\", \"type\": \"string\", \"analyzer\": \"snowball\", \"boost\": 1.0, \"store\": \"yes\"}}}}"));
        indexRandom(true,
                client().prepareIndex("$index", "$type", "data.activity.5").setSource("{\"django_ct\": \"data.activity\", \"grantee\": \"Grantee 1\"}"),
                client().prepareIndex("$index", "$type", "data.activity.6").setSource("{\"django_ct\": \"data.activity\", \"grantee\": \"Grantee 2\"}"));
        ensureYellow();
        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort("grantee", SortOrder.ASC)
                .execute().actionGet();
        assertOrderedSearchHits(searchResponse, "data.activity.5", "data.activity.6");
        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort("grantee", SortOrder.DESC)
                .execute().actionGet();
        assertOrderedSearchHits(searchResponse, "data.activity.6", "data.activity.5");
    }

    @Test
    public void testTrackScores() throws Exception {
        createIndex("test");
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
        Random random = getRandom();
        assertAcked(prepareCreate("test")
                .addMapping("type",
                        XContentFactory.jsonBuilder()
                                .startObject()
                                .startObject("type")
                                .startObject("properties")
                                .startObject("sparse_bytes")
                                .field("type", "string")
                                .field("index", "not_analyzed")
                                .endObject()
                                .startObject("dense_bytes")
                                .field("type", "string")
                                .field("index", "not_analyzed")
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
            assertThat(searchResponse.getHits().hits().length, equalTo(size));
            Set<Entry<BytesRef, String>> entrySet = denseBytes.entrySet();
            Iterator<Entry<BytesRef, String>> iterator = entrySet.iterator();
            for (int i = 0; i < size; i++) {
                assertThat(iterator.hasNext(), equalTo(true));
                Entry<BytesRef, String> next = iterator.next();
                assertThat("pos: " + i, searchResponse.getHits().getAt(i).id(), equalTo(next.getValue()));
                assertThat(searchResponse.getHits().getAt(i).sortValues()[0].toString(), equalTo(next.getKey().utf8ToString()));
            }
        }
        if (!sparseBytes.isEmpty()) {
            int size = between(1, sparseBytes.size());
            SearchResponse searchResponse = client().prepareSearch().setQuery(matchAllQuery())
                    .setPostFilter(QueryBuilders.existsQuery("sparse_bytes")).setSize(size).addSort("sparse_bytes", SortOrder.ASC).execute()
                    .actionGet();
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits(), equalTo((long) sparseBytes.size()));
            assertThat(searchResponse.getHits().hits().length, equalTo(size));
            Set<Entry<BytesRef, String>> entrySet = sparseBytes.entrySet();
            Iterator<Entry<BytesRef, String>> iterator = entrySet.iterator();
            for (int i = 0; i < size; i++) {
                assertThat(iterator.hasNext(), equalTo(true));
                Entry<BytesRef, String> next = iterator.next();
                assertThat(searchResponse.getHits().getAt(i).id(), equalTo(next.getValue()));
                assertThat(searchResponse.getHits().getAt(i).sortValues()[0].toString(), equalTo(next.getKey().utf8ToString()));
            }
        }
    }


    @Test
    public void test3078() {
        createIndex("test");
        ensureGreen();

        for (int i = 1; i < 101; i++) {
            client().prepareIndex("test", "type", Integer.toString(i)).setSource("field", Integer.toString(i)).execute().actionGet();
        }
        refresh();
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(matchAllQuery()).addSort(SortBuilders.fieldSort("field").order(SortOrder.ASC)).execute().actionGet();
        assertThat(searchResponse.getHits().getAt(0).sortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).sortValues()[0].toString(), equalTo("10"));
        assertThat(searchResponse.getHits().getAt(2).sortValues()[0].toString(), equalTo("100"));

        // reindex and refresh
        client().prepareIndex("test", "type", Integer.toString(1)).setSource("field", Integer.toString(1)).execute().actionGet();
        refresh();

        searchResponse = client().prepareSearch("test").setQuery(matchAllQuery()).addSort(SortBuilders.fieldSort("field").order(SortOrder.ASC)).execute().actionGet();
        assertThat(searchResponse.getHits().getAt(0).sortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).sortValues()[0].toString(), equalTo("10"));
        assertThat(searchResponse.getHits().getAt(2).sortValues()[0].toString(), equalTo("100"));

        // reindex - no refresh
        client().prepareIndex("test", "type", Integer.toString(1)).setSource("field", Integer.toString(1)).execute().actionGet();

        searchResponse = client().prepareSearch("test").setQuery(matchAllQuery()).addSort(SortBuilders.fieldSort("field").order(SortOrder.ASC)).execute().actionGet();
        assertThat(searchResponse.getHits().getAt(0).sortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).sortValues()[0].toString(), equalTo("10"));
        assertThat(searchResponse.getHits().getAt(2).sortValues()[0].toString(), equalTo("100"));

        // optimize
        optimize();
        refresh();

        client().prepareIndex("test", "type", Integer.toString(1)).setSource("field", Integer.toString(1)).execute().actionGet();
        searchResponse = client().prepareSearch("test").setQuery(matchAllQuery()).addSort(SortBuilders.fieldSort("field").order(SortOrder.ASC)).execute().actionGet();
        assertThat(searchResponse.getHits().getAt(0).sortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).sortValues()[0].toString(), equalTo("10"));
        assertThat(searchResponse.getHits().getAt(2).sortValues()[0].toString(), equalTo("100"));

        refresh();
        searchResponse = client().prepareSearch("test").setQuery(matchAllQuery()).addSort(SortBuilders.fieldSort("field").order(SortOrder.ASC)).execute().actionGet();
        assertThat(searchResponse.getHits().getAt(0).sortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).sortValues()[0].toString(), equalTo("10"));
        assertThat(searchResponse.getHits().getAt(2).sortValues()[0].toString(), equalTo("100"));
    }

    @Test
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
                        QueryBuilders.functionScoreQuery(matchAllQuery(), ScoreFunctionBuilders.scriptFunction(new Script("_source.field"))))
                .execute().actionGet();
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).score(), Matchers.lessThan(searchResponse.getHits().getAt(0).score()));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(2).score(), Matchers.lessThan(searchResponse.getHits().getAt(1).score()));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));

        searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        QueryBuilders.functionScoreQuery(matchAllQuery(), ScoreFunctionBuilders.scriptFunction(new Script("_source.field"))))
                .addSort("_score", SortOrder.DESC).execute().actionGet();
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).score(), Matchers.lessThan(searchResponse.getHits().getAt(0).score()));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(2).score(), Matchers.lessThan(searchResponse.getHits().getAt(1).score()));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));

        searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        QueryBuilders.functionScoreQuery(matchAllQuery(), ScoreFunctionBuilders.scriptFunction(new Script("_source.field"))))
                .addSort("_score", SortOrder.DESC).execute().actionGet();
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
    }


    @Test
    public void testScoreSortDirection_withFunctionScore() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "type", "1").setSource("field", 2).execute().actionGet();
        client().prepareIndex("test", "type", "2").setSource("field", 1).execute().actionGet();
        client().prepareIndex("test", "type", "3").setSource("field", 0).execute().actionGet();

        refresh();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(functionScoreQuery(matchAllQuery(), scriptFunction(new Script("_source.field")))).execute().actionGet();
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).score(), Matchers.lessThan(searchResponse.getHits().getAt(0).score()));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(2).score(), Matchers.lessThan(searchResponse.getHits().getAt(1).score()));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));

        searchResponse = client().prepareSearch("test")
                .setQuery(functionScoreQuery(matchAllQuery(), scriptFunction(new Script("_source.field"))))
                .addSort("_score", SortOrder.DESC).execute().actionGet();
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).score(), Matchers.lessThan(searchResponse.getHits().getAt(0).score()));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(2).score(), Matchers.lessThan(searchResponse.getHits().getAt(1).score()));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));

        searchResponse = client().prepareSearch("test")
                .setQuery(functionScoreQuery(matchAllQuery(), scriptFunction(new Script("_source.field"))))
                .addSort("_score", SortOrder.DESC).execute().actionGet();
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
    }

    @Test
    public void testIssue2986() {
        createIndex("test");

        client().prepareIndex("test", "post", "1").setSource("{\"field1\":\"value1\"}").execute().actionGet();
        client().prepareIndex("test", "post", "2").setSource("{\"field1\":\"value2\"}").execute().actionGet();
        client().prepareIndex("test", "post", "3").setSource("{\"field1\":\"value3\"}").execute().actionGet();
        refresh();
        SearchResponse result = client().prepareSearch("test").setQuery(matchAllQuery()).setTrackScores(true).addSort("field1", SortOrder.ASC).execute().actionGet();

        for (SearchHit hit : result.getHits()) {
            assertFalse(Float.isNaN(hit.getScore()));
        }
    }

    @Test
    public void testIssue2991() {
        for (int i = 1; i < 4; i++) {
            try {
                client().admin().indices().prepareDelete("test").execute().actionGet();
            } catch (Exception e) {
                // ignore
            }
            createIndex("test");
            ensureGreen();
            client().prepareIndex("test", "type", "1").setSource("tag", "alpha").execute().actionGet();
            refresh();

            client().prepareIndex("test", "type", "3").setSource("tag", "gamma").execute().actionGet();
            refresh();

            client().prepareIndex("test", "type", "4").setSource("tag", "delta").execute().actionGet();

            refresh();
            client().prepareIndex("test", "type", "2").setSource("tag", "beta").execute().actionGet();

            refresh();
            SearchResponse resp = client().prepareSearch("test").setSize(2).setQuery(matchAllQuery()).addSort(SortBuilders.fieldSort("tag").order(SortOrder.ASC)).execute().actionGet();
            assertHitCount(resp, 4);
            assertThat(resp.getHits().hits().length, equalTo(2));
            assertFirstHit(resp, hasId("1"));
            assertSecondHit(resp, hasId("2"));

            resp = client().prepareSearch("test").setSize(2).setQuery(matchAllQuery()).addSort(SortBuilders.fieldSort("tag").order(SortOrder.DESC)).execute().actionGet();
            assertHitCount(resp, 4);
            assertThat(resp.getHits().hits().length, equalTo(2));
            assertFirstHit(resp, hasId("3"));
            assertSecondHit(resp, hasId("4"));
        }
    }

    @Test
    public void testSimpleSorts() throws Exception {
        Random random = getRandom();
        assertAcked(prepareCreate("test")
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("str_value").field("type", "string").field("index", "not_analyzed").startObject("fielddata").field("format", random().nextBoolean() ? "doc_values" : null).endObject().endObject()
                        .startObject("boolean_value").field("type", "boolean").endObject()
                        .startObject("byte_value").field("type", "byte").startObject("fielddata").field("format", random().nextBoolean() ? "doc_values" : null).endObject().endObject()
                        .startObject("short_value").field("type", "short").startObject("fielddata").field("format", random().nextBoolean() ? "doc_values" : null).endObject().endObject()
                        .startObject("integer_value").field("type", "integer").startObject("fielddata").field("format", random().nextBoolean() ? "doc_values" : null).endObject().endObject()
                        .startObject("long_value").field("type", "long").startObject("fielddata").field("format", random().nextBoolean() ? "doc_values" : null).endObject().endObject()
                        .startObject("float_value").field("type", "float").startObject("fielddata").field("format", random().nextBoolean() ? "doc_values" : null).endObject().endObject()
                        .startObject("double_value").field("type", "double").startObject("fielddata").field("format", random().nextBoolean() ? "doc_values" : null).endObject().endObject()
                        .endObject().endObject().endObject()));
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
        assertThat(searchResponse.getHits().hits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).id(), equalTo(Integer.toString(i)));
            assertThat(searchResponse.getHits().getAt(i).sortValues()[0].toString(), equalTo(new String(new char[]{(char) (97 + i), (char) (97 + i)})));
        }
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(size)
                .addSort("str_value", SortOrder.DESC)
                .execute().actionGet();

        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().hits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).id(), equalTo(Integer.toString(9 - i)));
            assertThat(searchResponse.getHits().getAt(i).sortValues()[0].toString(), equalTo(new String(new char[]{(char) (97 + (9 - i)), (char) (97 + (9 - i))})));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));


        // STRING script
        size = 1 + random.nextInt(10);

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(size)
                .addSort(new ScriptSortBuilder(new Script("doc['str_value'].value"), "string")).execute().actionGet();
        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().hits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).id(), equalTo(Integer.toString(i)));
            assertThat(searchResponse.getHits().getAt(i).sortValues()[0].toString(), equalTo(new String(new char[] { (char) (97 + i),
                    (char) (97 + i) })));
        }
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("str_value", SortOrder.DESC).execute()
                .actionGet();

        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().hits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).id(), equalTo(Integer.toString(9 - i)));
            assertThat(searchResponse.getHits().getAt(i).sortValues()[0].toString(), equalTo(new String(new char[] { (char) (97 + (9 - i)),
                    (char) (97 + (9 - i)) })));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));

        // BYTE
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("byte_value", SortOrder.ASC).execute()
                .actionGet();

        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().hits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).id(), equalTo(Integer.toString(i)));
            assertThat(((Number) searchResponse.getHits().getAt(i).sortValues()[0]).byteValue(), equalTo((byte) i));
        }
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("byte_value", SortOrder.DESC).execute()
                .actionGet();

        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().hits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).id(), equalTo(Integer.toString(9 - i)));
            assertThat(((Number) searchResponse.getHits().getAt(i).sortValues()[0]).byteValue(), equalTo((byte) (9 - i)));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));

        // SHORT
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("short_value", SortOrder.ASC).execute()
                .actionGet();

        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().hits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).id(), equalTo(Integer.toString(i)));
            assertThat(((Number) searchResponse.getHits().getAt(i).sortValues()[0]).shortValue(), equalTo((short) i));
        }
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("short_value", SortOrder.DESC).execute()
                .actionGet();

        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().hits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).id(), equalTo(Integer.toString(9 - i)));
            assertThat(((Number) searchResponse.getHits().getAt(i).sortValues()[0]).shortValue(), equalTo((short) (9 - i)));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));

        // INTEGER
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("integer_value", SortOrder.ASC).execute()
                .actionGet();

        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().hits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).id(), equalTo(Integer.toString(i)));
            assertThat(((Number) searchResponse.getHits().getAt(i).sortValues()[0]).intValue(), equalTo(i));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("integer_value", SortOrder.DESC)
                .execute().actionGet();

        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().hits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).id(), equalTo(Integer.toString(9 - i)));
            assertThat(((Number) searchResponse.getHits().getAt(i).sortValues()[0]).intValue(), equalTo((9 - i)));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));

        // LONG
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("long_value", SortOrder.ASC).execute()
                .actionGet();

        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().hits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).id(), equalTo(Integer.toString(i)));
            assertThat(((Number) searchResponse.getHits().getAt(i).sortValues()[0]).longValue(), equalTo((long) i));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("long_value", SortOrder.DESC).execute()
                .actionGet();
        assertHitCount(searchResponse, 10l);
        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().hits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).id(), equalTo(Integer.toString(9 - i)));
            assertThat(((Number) searchResponse.getHits().getAt(i).sortValues()[0]).longValue(), equalTo((long) (9 - i)));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));

        // FLOAT
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("float_value", SortOrder.ASC).execute()
                .actionGet();

        assertHitCount(searchResponse, 10l);
        assertThat(searchResponse.getHits().hits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).id(), equalTo(Integer.toString(i)));
            assertThat(((Number) searchResponse.getHits().getAt(i).sortValues()[0]).doubleValue(), closeTo(0.1d * i, 0.000001d));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("float_value", SortOrder.DESC).execute()
                .actionGet();

        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().hits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).id(), equalTo(Integer.toString(9 - i)));
            assertThat(((Number) searchResponse.getHits().getAt(i).sortValues()[0]).doubleValue(), closeTo(0.1d * (9 - i), 0.000001d));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));

        // DOUBLE
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("double_value", SortOrder.ASC).execute()
                .actionGet();

        assertHitCount(searchResponse, 10l);
        assertThat(searchResponse.getHits().hits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).id(), equalTo(Integer.toString(i)));
            assertThat(((Number) searchResponse.getHits().getAt(i).sortValues()[0]).doubleValue(), closeTo(0.1d * i, 0.000001d));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("double_value", SortOrder.DESC).execute()
                .actionGet();

        assertHitCount(searchResponse, 10l);
        assertThat(searchResponse.getHits().hits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).id(), equalTo(Integer.toString(9 - i)));
            assertThat(((Number) searchResponse.getHits().getAt(i).sortValues()[0]).doubleValue(), closeTo(0.1d * (9 - i), 0.000001d));
        }

        assertNoFailures(searchResponse);
    }

    @Test
    public void test2920() throws IOException {
        assertAcked(prepareCreate("test").addMapping(
                "test",
                jsonBuilder().startObject().startObject("test").startObject("properties").startObject("value").field("type", "string")
                        .endObject().endObject().endObject().endObject()));
        ensureGreen();
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "test", Integer.toString(i))
                    .setSource(jsonBuilder().startObject().field("value", "" + i).endObject()).execute().actionGet();
        }
        refresh();
        SearchResponse searchResponse = client().prepareSearch().setQuery(matchAllQuery())
                .addSort(SortBuilders.scriptSort(new Script("\u0027\u0027"), "string")).setSize(10).execute().actionGet();
        assertNoFailures(searchResponse);
    }

    @Test
    public void testSortMinValueScript() throws IOException {
        String mapping = jsonBuilder().startObject().startObject("type1").startObject("properties")
                .startObject("lvalue").field("type", "long").endObject()
                .startObject("dvalue").field("type", "double").endObject()
                .startObject("svalue").field("type", "string").endObject()
                .startObject("gvalue").field("type", "geo_point").endObject()
                .endObject().endObject().endObject().string();
        assertAcked(prepareCreate("test").addMapping("type1", mapping));
        ensureGreen();

        for (int i = 0; i < 10; i++) {
            IndexRequestBuilder req = client().prepareIndex("test", "type1", "" + i).setSource(jsonBuilder().startObject()
                    .field("ord", i)
                    .field("svalue", new String[]{"" + i, "" + (i + 1), "" + (i + 2)})
                    .field("lvalue", new long[]{i, i + 1, i + 2})
                    .field("dvalue", new double[]{i, i + 1, i + 2})
                    .startObject("gvalue")
                    .field("lat", (double) i + 1)
                    .field("lon", (double) i)
                    .endObject()
                    .endObject());
            req.execute().actionGet();
        }

        for (int i = 10; i < 20; i++) { // add some docs that don't have values in those fields
            client().prepareIndex("test", "type1", "" + i).setSource(jsonBuilder().startObject()
                    .field("ord", i)
                    .endObject()).execute().actionGet();
        }
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        // test the long values
        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addScriptField("min", new Script("retval = Long.MAX_VALUE; for (v in doc['lvalue'].values){ retval = min(v, retval) }; retval"))
                .addSort(SortBuilders.fieldSort("ord").order(SortOrder.ASC).unmappedType("long")).setSize(10)
                .execute().actionGet();

        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(20l));
        for (int i = 0; i < 10; i++) {
            assertThat("res: " + i + " id: " + searchResponse.getHits().getAt(i).getId(), (Long) searchResponse.getHits().getAt(i).field("min").value(), equalTo((long) i));
        }
        // test the double values
        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addScriptField("min", new Script("retval = Double.MAX_VALUE; for (v in doc['dvalue'].values){ retval = min(v, retval) }; retval"))
                .addSort(SortBuilders.fieldSort("ord").order(SortOrder.ASC).unmappedType("long")).setSize(10)
                .execute().actionGet();

        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(20l));
        for (int i = 0; i < 10; i++) {
            assertThat("res: " + i + " id: " + searchResponse.getHits().getAt(i).getId(), (Double) searchResponse.getHits().getAt(i).field("min").value(), equalTo((double) i));
        }

        // test the string values
        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addScriptField("min", new Script("retval = Integer.MAX_VALUE; for (v in doc['svalue'].values){ retval = min(Integer.parseInt(v), retval) }; retval"))
                .addSort(SortBuilders.fieldSort("ord").order(SortOrder.ASC).unmappedType("long")).setSize(10)
                .execute().actionGet();

        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(20l));
        for (int i = 0; i < 10; i++) {
            assertThat("res: " + i + " id: " + searchResponse.getHits().getAt(i).getId(), (Integer) searchResponse.getHits().getAt(i).field("min").value(), equalTo(i));
        }

        // test the geopoint values
        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addScriptField("min", new Script("retval = Double.MAX_VALUE; for (v in doc['gvalue'].values){ retval = min(v.lon, retval) }; retval"))
                .addSort(SortBuilders.fieldSort("ord").order(SortOrder.ASC).unmappedType("long")).setSize(10)
                .execute().actionGet();

        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(20l));
        for (int i = 0; i < 10; i++) {
            assertThat("res: " + i + " id: " + searchResponse.getHits().getAt(i).getId(), (Double) searchResponse.getHits().getAt(i).field("min").value(), equalTo((double) i));
        }
    }

    @Test
    public void testDocumentsWithNullValue() throws Exception {
        // TODO: sort shouldn't fail when sort field is mapped dynamically
        // We have to specify mapping explicitly because by the time search is performed dynamic mapping might not
        // be propagated to all nodes yet and sort operation fail when the sort field is not defined
        String mapping = jsonBuilder().startObject().startObject("type1").startObject("properties")
                .startObject("svalue").field("type", "string").field("index", "not_analyzed").startObject("fielddata").field("format", random().nextBoolean() ? "doc_values" : null).endObject().endObject()
                .endObject().endObject().endObject().string();
        assertAcked(prepareCreate("test").addMapping("type1", mapping));
        ensureGreen();

        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("id", "1")
                .field("svalue", "aaa")
                .endObject()).execute().actionGet();

        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("id", "2")
                .nullField("svalue")
                .endObject()).execute().actionGet();

        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("id", "3")
                .field("svalue", "bbb")
                .endObject()).execute().actionGet();


        flush();
        refresh();

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addScriptField("id", new Script("doc['id'].value"))
                .addSort("svalue", SortOrder.ASC)
                .execute().actionGet();

        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3l));
        assertThat((String) searchResponse.getHits().getAt(0).field("id").value(), equalTo("1"));
        assertThat((String) searchResponse.getHits().getAt(1).field("id").value(), equalTo("3"));
        assertThat((String) searchResponse.getHits().getAt(2).field("id").value(), equalTo("2"));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addScriptField("id", new Script("doc['id'].values[0]"))
                .addSort("svalue", SortOrder.ASC)
                .execute().actionGet();

        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3l));
        assertThat((String) searchResponse.getHits().getAt(0).field("id").value(), equalTo("1"));
        assertThat((String) searchResponse.getHits().getAt(1).field("id").value(), equalTo("3"));
        assertThat((String) searchResponse.getHits().getAt(2).field("id").value(), equalTo("2"));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addScriptField("id", new Script("doc['id'].value"))
                .addSort("svalue", SortOrder.DESC)
                .execute().actionGet();

        if (searchResponse.getFailedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.getShardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.getFailedShards(), equalTo(0));

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3l));
        assertThat((String) searchResponse.getHits().getAt(0).field("id").value(), equalTo("3"));
        assertThat((String) searchResponse.getHits().getAt(1).field("id").value(), equalTo("1"));
        assertThat((String) searchResponse.getHits().getAt(2).field("id").value(), equalTo("2"));

        // a query with docs just with null values
        searchResponse = client().prepareSearch()
                .setQuery(termQuery("id", "2"))
                .addScriptField("id", new Script("doc['id'].value"))
                .addSort("svalue", SortOrder.DESC)
                .execute().actionGet();

        if (searchResponse.getFailedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.getShardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.getFailedShards(), equalTo(0));

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat((String) searchResponse.getHits().getAt(0).field("id").value(), equalTo("2"));
    }

    @Test
    public void testSortMissingNumbers() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type1",
                XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("type1")
                        .startObject("properties")
                            .startObject("i_value")
                                .field("type", "integer")
                                .startObject("fielddata").field("format", random().nextBoolean() ? "doc_values" : null).endObject()
                            .endObject()
                            .startObject("d_value")
                                .field("type", "float")
                                .startObject("fielddata").field("format", random().nextBoolean() ? "doc_values" : null).endObject()
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

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(2).id(), equalTo("2"));

        logger.info("--> sort with missing _last");
        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(SortBuilders.fieldSort("i_value").order(SortOrder.ASC).missing("_last"))
                .execute().actionGet();
        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(2).id(), equalTo("2"));

        logger.info("--> sort with missing _first");
        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(SortBuilders.fieldSort("i_value").order(SortOrder.ASC).missing("_first"))
                .execute().actionGet();
        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(2).id(), equalTo("3"));
    }

    @Test
    public void testSortMissingStrings() throws IOException {
        assertAcked(prepareCreate("test").addMapping("type1",
                XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("type1")
                        .startObject("properties")
                        .startObject("value")
                        .field("type", "string")
                        .field("index", "not_analyzed")
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

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(2).id(), equalTo("2"));

        logger.info("--> sort with missing _last");
        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(SortBuilders.fieldSort("value").order(SortOrder.ASC).missing("_last"))
                .execute().actionGet();
        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(2).id(), equalTo("2"));

        logger.info("--> sort with missing _first");
        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(SortBuilders.fieldSort("value").order(SortOrder.ASC).missing("_first"))
                .execute().actionGet();
        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(2).id(), equalTo("3"));

        logger.info("--> sort with missing b");
        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(SortBuilders.fieldSort("value").order(SortOrder.ASC).missing("b"))
                .execute().actionGet();
        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(2).id(), equalTo("3"));
    }

    @Test
    public void testIgnoreUnmapped() throws Exception {
        createIndex("test");
        ensureYellow();

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
                .addSort(SortBuilders.fieldSort("kkk").unmappedType("string"))
                .execute().actionGet();
        assertNoFailures(searchResponse);
    }

    @Test
    public void testSortMVField() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("long_values").field("type", "long").startObject("fielddata").field("format", random().nextBoolean() ? "doc_values" : null).endObject().endObject()
                        .startObject("int_values").field("type", "integer").startObject("fielddata").field("format", random().nextBoolean() ? "doc_values" : null).endObject().endObject()
                        .startObject("short_values").field("type", "short").startObject("fielddata").field("format", random().nextBoolean() ? "doc_values" : null).endObject().endObject()
                        .startObject("byte_values").field("type", "byte").startObject("fielddata").field("format", random().nextBoolean() ? "doc_values" : null).endObject().endObject()
                        .startObject("float_values").field("type", "float").startObject("fielddata").field("format", random().nextBoolean() ? "doc_values" : null).endObject().endObject()
                        .startObject("double_values").field("type", "double").startObject("fielddata").field("format", random().nextBoolean() ? "doc_values" : null).endObject().endObject()
                        .startObject("string_values").field("type", "string").field("index", "not_analyzed").startObject("fielddata").field("format", random().nextBoolean() ? "doc_values" : null).endObject().endObject()
                        .endObject().endObject().endObject()));
        ensureGreen();

        client().prepareIndex("test", "type1", Integer.toString(1)).setSource(jsonBuilder().startObject()
                .array("long_values", 1l, 5l, 10l, 8l)
                .array("int_values", 1, 5, 10, 8)
                .array("short_values", 1, 5, 10, 8)
                .array("byte_values", 1, 5, 10, 8)
                .array("float_values", 1f, 5f, 10f, 8f)
                .array("double_values", 1d, 5d, 10d, 8d)
                .array("string_values", "01", "05", "10", "08")
                .endObject()).execute().actionGet();
        client().prepareIndex("test", "type1", Integer.toString(2)).setSource(jsonBuilder().startObject()
                .array("long_values", 11l, 15l, 20l, 7l)
                .array("int_values", 11, 15, 20, 7)
                .array("short_values", 11, 15, 20, 7)
                .array("byte_values", 11, 15, 20, 7)
                .array("float_values", 11f, 15f, 20f, 7f)
                .array("double_values", 11d, 15d, 20d, 7d)
                .array("string_values", "11", "15", "20", "07")
                .endObject()).execute().actionGet();
        client().prepareIndex("test", "type1", Integer.toString(3)).setSource(jsonBuilder().startObject()
                .array("long_values", 2l, 1l, 3l, -4l)
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

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3l));
        assertThat(searchResponse.getHits().hits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).id(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).longValue(), equalTo(-4l));

        assertThat(searchResponse.getHits().getAt(1).id(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).longValue(), equalTo(1l));

        assertThat(searchResponse.getHits().getAt(2).id(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).longValue(), equalTo(7l));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("long_values", SortOrder.DESC)
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3l));
        assertThat(searchResponse.getHits().hits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).id(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).longValue(), equalTo(20l));

        assertThat(searchResponse.getHits().getAt(1).id(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).longValue(), equalTo(10l));

        assertThat(searchResponse.getHits().getAt(2).id(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).longValue(), equalTo(3l));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort(SortBuilders.fieldSort("long_values").order(SortOrder.DESC).sortMode("sum"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3l));
        assertThat(searchResponse.getHits().hits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).id(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).longValue(), equalTo(53l));

        assertThat(searchResponse.getHits().getAt(1).id(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).longValue(), equalTo(24l));

        assertThat(searchResponse.getHits().getAt(2).id(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).longValue(), equalTo(2l));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("int_values", SortOrder.ASC)
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3l));
        assertThat(searchResponse.getHits().hits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).id(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).intValue(), equalTo(-4));

        assertThat(searchResponse.getHits().getAt(1).id(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).intValue(), equalTo(1));

        assertThat(searchResponse.getHits().getAt(2).id(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).intValue(), equalTo(7));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("int_values", SortOrder.DESC)
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3l));
        assertThat(searchResponse.getHits().hits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).id(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).intValue(), equalTo(20));

        assertThat(searchResponse.getHits().getAt(1).id(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).intValue(), equalTo(10));

        assertThat(searchResponse.getHits().getAt(2).id(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).intValue(), equalTo(3));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("short_values", SortOrder.ASC)
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3l));
        assertThat(searchResponse.getHits().hits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).id(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).intValue(), equalTo(-4));

        assertThat(searchResponse.getHits().getAt(1).id(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).intValue(), equalTo(1));

        assertThat(searchResponse.getHits().getAt(2).id(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).intValue(), equalTo(7));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("short_values", SortOrder.DESC)
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3l));
        assertThat(searchResponse.getHits().hits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).id(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).intValue(), equalTo(20));

        assertThat(searchResponse.getHits().getAt(1).id(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).intValue(), equalTo(10));

        assertThat(searchResponse.getHits().getAt(2).id(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).intValue(), equalTo(3));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("byte_values", SortOrder.ASC)
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3l));
        assertThat(searchResponse.getHits().hits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).id(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).intValue(), equalTo(-4));

        assertThat(searchResponse.getHits().getAt(1).id(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).intValue(), equalTo(1));

        assertThat(searchResponse.getHits().getAt(2).id(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).intValue(), equalTo(7));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("byte_values", SortOrder.DESC)
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3l));
        assertThat(searchResponse.getHits().hits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).id(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).intValue(), equalTo(20));

        assertThat(searchResponse.getHits().getAt(1).id(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).intValue(), equalTo(10));

        assertThat(searchResponse.getHits().getAt(2).id(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).intValue(), equalTo(3));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("float_values", SortOrder.ASC)
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3l));
        assertThat(searchResponse.getHits().hits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).id(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).floatValue(), equalTo(-4f));

        assertThat(searchResponse.getHits().getAt(1).id(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).floatValue(), equalTo(1f));

        assertThat(searchResponse.getHits().getAt(2).id(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).floatValue(), equalTo(7f));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("float_values", SortOrder.DESC)
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3l));
        assertThat(searchResponse.getHits().hits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).id(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).floatValue(), equalTo(20f));

        assertThat(searchResponse.getHits().getAt(1).id(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).floatValue(), equalTo(10f));

        assertThat(searchResponse.getHits().getAt(2).id(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).floatValue(), equalTo(3f));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("double_values", SortOrder.ASC)
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3l));
        assertThat(searchResponse.getHits().hits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).id(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), equalTo(-4d));

        assertThat(searchResponse.getHits().getAt(1).id(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), equalTo(1d));

        assertThat(searchResponse.getHits().getAt(2).id(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).doubleValue(), equalTo(7d));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("double_values", SortOrder.DESC)
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3l));
        assertThat(searchResponse.getHits().hits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).id(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), equalTo(20d));

        assertThat(searchResponse.getHits().getAt(1).id(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), equalTo(10d));

        assertThat(searchResponse.getHits().getAt(2).id(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).doubleValue(), equalTo(3d));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("string_values", SortOrder.ASC)
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3l));
        assertThat(searchResponse.getHits().hits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).id(), equalTo(Integer.toString(3)));
        assertThat(((Text) searchResponse.getHits().getAt(0).sortValues()[0]).string(), equalTo("!4"));

        assertThat(searchResponse.getHits().getAt(1).id(), equalTo(Integer.toString(1)));
        assertThat(((Text) searchResponse.getHits().getAt(1).sortValues()[0]).string(), equalTo("01"));

        assertThat(searchResponse.getHits().getAt(2).id(), equalTo(Integer.toString(2)));
        assertThat(((Text) searchResponse.getHits().getAt(2).sortValues()[0]).string(), equalTo("07"));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(10)
                .addSort("string_values", SortOrder.DESC)
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3l));
        assertThat(searchResponse.getHits().hits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).id(), equalTo(Integer.toString(2)));
        assertThat(((Text) searchResponse.getHits().getAt(0).sortValues()[0]).string(), equalTo("20"));

        assertThat(searchResponse.getHits().getAt(1).id(), equalTo(Integer.toString(1)));
        assertThat(((Text) searchResponse.getHits().getAt(1).sortValues()[0]).string(), equalTo("10"));

        assertThat(searchResponse.getHits().getAt(2).id(), equalTo(Integer.toString(3)));
        assertThat(((Text) searchResponse.getHits().getAt(2).sortValues()[0]).string(), equalTo("03"));
    }

    @Test
    public void testSortOnRareField() throws IOException {
        assertAcked(prepareCreate("test")
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("string_values").field("type", "string").field("index", "not_analyzed").startObject("fielddata").field("format", random().nextBoolean() ? "doc_values" : null).endObject().endObject()
                        .endObject().endObject().endObject()));
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

        assertThat(searchResponse.getHits().hits().length, equalTo(1));


        assertThat(searchResponse.getHits().getAt(0).id(), equalTo(Integer.toString(1)));
        assertThat(((Text) searchResponse.getHits().getAt(0).sortValues()[0]).string(), equalTo("10"));

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

        assertThat(searchResponse.getHits().hits().length, equalTo(2));

        assertThat(searchResponse.getHits().getAt(0).id(), equalTo(Integer.toString(2)));
        assertThat(((Text) searchResponse.getHits().getAt(0).sortValues()[0]).string(), equalTo("20"));

        assertThat(searchResponse.getHits().getAt(1).id(), equalTo(Integer.toString(1)));
        assertThat(((Text) searchResponse.getHits().getAt(1).sortValues()[0]).string(), equalTo("10"));


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

        assertThat(searchResponse.getHits().hits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).id(), equalTo(Integer.toString(2)));
        assertThat(((Text) searchResponse.getHits().getAt(0).sortValues()[0]).string(), equalTo("20"));

        assertThat(searchResponse.getHits().getAt(1).id(), equalTo(Integer.toString(1)));
        assertThat(((Text) searchResponse.getHits().getAt(1).sortValues()[0]).string(), equalTo("10"));

        assertThat(searchResponse.getHits().getAt(2).id(), equalTo(Integer.toString(3)));
        assertThat(((Text) searchResponse.getHits().getAt(2).sortValues()[0]).string(), equalTo("03"));

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

        assertThat(searchResponse.getHits().hits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).id(), equalTo(Integer.toString(2)));
        assertThat(((Text) searchResponse.getHits().getAt(0).sortValues()[0]).string(), equalTo("20"));

        assertThat(searchResponse.getHits().getAt(1).id(), equalTo(Integer.toString(1)));
        assertThat(((Text) searchResponse.getHits().getAt(1).sortValues()[0]).string(), equalTo("10"));

        assertThat(searchResponse.getHits().getAt(2).id(), equalTo(Integer.toString(3)));
        assertThat(((Text) searchResponse.getHits().getAt(2).sortValues()[0]).string(), equalTo("03"));
    }

    public void testSortMetaField() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp").field("enabled", true).endObject()
                .endObject().endObject();
        assertAcked(prepareCreate("test")
            .addMapping("type", mapping));
        ensureGreen();
        final int numDocs = randomIntBetween(10, 20);
        IndexRequestBuilder[] indexReqs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; ++i) {
            indexReqs[i] = client().prepareIndex("test", "type", Integer.toString(i)).setTimestamp(Integer.toString(randomInt(1000))).setSource();
        }
        indexRandom(true, indexReqs);

        SortOrder order = randomFrom(SortOrder.values());
        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(randomIntBetween(1, numDocs + 5))
                .addSort("_uid", order)
                .execute().actionGet();
        assertNoFailures(searchResponse);
        SearchHit[] hits = searchResponse.getHits().hits();
        BytesRef previous = order == SortOrder.ASC ? new BytesRef() : UnicodeUtil.BIG_TERM;
        for (int i = 0; i < hits.length; ++i) {
            final BytesRef uid = new BytesRef(Uid.createUid(hits[i].type(), hits[i].id()));
            assertThat(previous, order == SortOrder.ASC ? lessThan(uid) : greaterThan(uid));
            previous = uid;
        }

        /*
        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(randomIntBetween(1, numDocs + 5))
                .addSort("_id", order)
                .execute().actionGet();
        assertNoFailures(searchResponse);
        hits = searchResponse.getHits().hits();
        previous = order == SortOrder.ASC ? new BytesRef() : UnicodeUtil.BIG_TERM;
        for (int i = 0; i < hits.length; ++i) {
            final BytesRef id = new BytesRef(Uid.createUid(hits[i].type(), hits[i].id()));
            assertThat(previous, order == SortOrder.ASC ? lessThan(id) : greaterThan(id));
            previous = id;
        }*/

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(randomIntBetween(1, numDocs + 5))
                .addSort("_timestamp", order)
                .addField("_timestamp")
                .execute().actionGet();
        assertNoFailures(searchResponse);
        hits = searchResponse.getHits().hits();
        Long previousTs = order == SortOrder.ASC ? 0 : Long.MAX_VALUE;
        for (int i = 0; i < hits.length; ++i) {
            SearchHitField timestampField = hits[i].getFields().get("_timestamp");
            Long timestamp = timestampField.<Long>getValue();
            assertThat(previousTs, order == SortOrder.ASC ? lessThanOrEqualTo(timestamp) : greaterThanOrEqualTo(timestamp));
            previousTs = timestamp;
        }
    }

    /**
     * Test case for issue 6150: https://github.com/elasticsearch/elasticsearch/issues/6150
     */
    @Test
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
                                                        .field("type", "string")
                                                        .startObject("fields")
                                                            .startObject("sub")
                                                                .field("type", "string")
                                                                .field("index", "not_analyzed")
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
        SearchHit[] hits = searchResponse.getHits().hits();
        for (int i = 0; i < hits.length; ++i) {
            assertThat(hits[i].getSortValues().length, is(1));
            Object o = hits[i].getSortValues()[0];
            assertThat(o, notNullValue());
            assertThat(o instanceof StringAndBytesText, is(true));
            StringAndBytesText text = (StringAndBytesText) o;
            assertThat(text.string(), is("bar"));
        }


        // We sort on nested sub field
        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(SortBuilders.fieldSort("nested.foo.sub").setNestedPath("nested").order(SortOrder.DESC))
                .execute().actionGet();
        assertNoFailures(searchResponse);
        hits = searchResponse.getHits().hits();
        for (int i = 0; i < hits.length; ++i) {
            assertThat(hits[i].getSortValues().length, is(1));
            Object o = hits[i].getSortValues()[0];
            assertThat(o, notNullValue());
            assertThat(o instanceof StringAndBytesText, is(true));
            StringAndBytesText text = (StringAndBytesText) o;
            assertThat(text.string(), is("bar bar"));
        }
    }

    @Test
    public void testSortDuelBetweenSingleShardAndMultiShardIndex() throws Exception {
        String sortField = "sortField";
        assertAcked(prepareCreate("test1")
                .setSettings(IndexMetaData.SETTING_NUMBER_OF_SHARDS, between(2, maximumNumberOfShards()))
                .addMapping("type", sortField, "type=long").get());
        assertAcked(prepareCreate("test2")
                .setSettings(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
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

        assertThat(multiShardResponse.getHits().totalHits(), equalTo(singleShardResponse.getHits().totalHits()));
        assertThat(multiShardResponse.getHits().getHits().length, equalTo(singleShardResponse.getHits().getHits().length));
        for (int i = 0; i < multiShardResponse.getHits().getHits().length; i++) {
            assertThat(multiShardResponse.getHits().getAt(i).sortValues()[0], equalTo(singleShardResponse.getHits().getAt(i).sortValues()[0]));
            assertThat(multiShardResponse.getHits().getAt(i).id(), equalTo(singleShardResponse.getHits().getAt(i).id()));
        }
    }

    public void testManyToManyGeoPoints() throws ExecutionException, InterruptedException, IOException {
        /**
         * | q  |  d1    |   d2
         * |    |        |
         * |    |        |
         * |    |        |
         * |2  o|  x     |     x
         * |    |        |
         * |1  o|      x | x
         * |___________________________
         * 1   2   3   4   5   6   7
         */
        assertAcked(prepareCreate("index").addMapping("type", "location", "type=geo_point"));
        XContentBuilder d1Builder = jsonBuilder();
        GeoPoint[] d1Points = {new GeoPoint(3, 2), new GeoPoint(4, 1)};
        createShuffeldJSONArray(d1Builder, d1Points);

        XContentBuilder d2Builder = jsonBuilder();
        GeoPoint[] d2Points = {new GeoPoint(5, 1), new GeoPoint(6, 2)};
        createShuffeldJSONArray(d2Builder, d2Points);

        logger.info(d1Builder.string());
        logger.info(d2Builder.string());
        indexRandom(true,
                client().prepareIndex("index", "type", "d1").setSource(d1Builder),
                client().prepareIndex("index", "type", "d2").setSource(d2Builder));
        ensureYellow();
        GeoPoint[] q = new GeoPoint[2];
        if (randomBoolean()) {
            q[0] = new GeoPoint(2, 1);
            q[1] = new GeoPoint(2, 2);
        } else {
            q[1] = new GeoPoint(2, 2);
            q[0] = new GeoPoint(2, 1);
        }

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(new GeoDistanceSortBuilder("location").points(q).sortMode("min").order(SortOrder.ASC).geoDistance(GeoDistance.PLANE).unit(DistanceUnit.KILOMETERS))
                .execute().actionGet();
        assertOrderedSearchHits(searchResponse, "d1", "d2");
        assertThat((Double) searchResponse.getHits().getAt(0).getSortValues()[0], equalTo(GeoDistance.PLANE.calculate(2, 2, 3, 2, DistanceUnit.KILOMETERS)));
        assertThat((Double) searchResponse.getHits().getAt(1).getSortValues()[0], equalTo(GeoDistance.PLANE.calculate(2, 1, 5, 1, DistanceUnit.KILOMETERS)));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(new GeoDistanceSortBuilder("location").points(q).sortMode("min").order(SortOrder.DESC).geoDistance(GeoDistance.PLANE).unit(DistanceUnit.KILOMETERS))
                .execute().actionGet();
        assertOrderedSearchHits(searchResponse, "d2", "d1");
        assertThat((Double) searchResponse.getHits().getAt(0).getSortValues()[0], equalTo(GeoDistance.PLANE.calculate(2, 1, 5, 1, DistanceUnit.KILOMETERS)));
        assertThat((Double) searchResponse.getHits().getAt(1).getSortValues()[0], equalTo(GeoDistance.PLANE.calculate(2, 2, 3, 2, DistanceUnit.KILOMETERS)));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(new GeoDistanceSortBuilder("location").points(q).sortMode("max").order(SortOrder.ASC).geoDistance(GeoDistance.PLANE).unit(DistanceUnit.KILOMETERS))
                .execute().actionGet();
        assertOrderedSearchHits(searchResponse, "d1", "d2");
        assertThat((Double) searchResponse.getHits().getAt(0).getSortValues()[0], equalTo(GeoDistance.PLANE.calculate(2, 2, 4, 1, DistanceUnit.KILOMETERS)));
        assertThat((Double) searchResponse.getHits().getAt(1).getSortValues()[0], equalTo(GeoDistance.PLANE.calculate(2, 1, 6, 2, DistanceUnit.KILOMETERS)));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(new GeoDistanceSortBuilder("location").points(q).sortMode("max").order(SortOrder.DESC).geoDistance(GeoDistance.PLANE).unit(DistanceUnit.KILOMETERS))
                .execute().actionGet();
        assertOrderedSearchHits(searchResponse, "d2", "d1");
        assertThat((Double) searchResponse.getHits().getAt(0).getSortValues()[0], equalTo(GeoDistance.PLANE.calculate(2, 1, 6, 2, DistanceUnit.KILOMETERS)));
        assertThat((Double) searchResponse.getHits().getAt(1).getSortValues()[0], equalTo(GeoDistance.PLANE.calculate(2, 2, 4, 1, DistanceUnit.KILOMETERS)));
    }

    protected void createShuffeldJSONArray(XContentBuilder builder, GeoPoint[] pointsArray) throws IOException {
        List<GeoPoint> points = new ArrayList<>();
        points.addAll(Arrays.asList(pointsArray));
        builder.startObject();
        builder.startArray("location");
        int numPoints = points.size();
        for (int i = 0; i < numPoints; i++) {
            builder.value(points.remove(randomInt(points.size() - 1)));
        }
        builder.endArray();
        builder.endObject();
    }

    public void testManyToManyGeoPointsWithDifferentFormats() throws ExecutionException, InterruptedException, IOException {
        /**   q     d1       d2
         * |4  o|   x    |   x
         * |    |        |
         * |3  o|  x     |  x
         * |    |        |
         * |2  o| x      | x
         * |    |        |
         * |1  o|x       |x
         * |______________________
         * 1   2   3   4   5   6
         */
        assertAcked(prepareCreate("index").addMapping("type", "location", "type=geo_point"));
        XContentBuilder d1Builder = jsonBuilder();
        GeoPoint[] d1Points = {new GeoPoint(2.5, 1), new GeoPoint(2.75, 2), new GeoPoint(3, 3), new GeoPoint(3.25, 4)};
        createShuffeldJSONArray(d1Builder, d1Points);

        XContentBuilder d2Builder = jsonBuilder();
        GeoPoint[] d2Points = {new GeoPoint(4.5, 1), new GeoPoint(4.75, 2), new GeoPoint(5, 3), new GeoPoint(5.25, 4)};
        createShuffeldJSONArray(d2Builder, d2Points);

        indexRandom(true,
                client().prepareIndex("index", "type", "d1").setSource(d1Builder),
                client().prepareIndex("index", "type", "d2").setSource(d2Builder));
        ensureYellow();

        List<String> qHashes = new ArrayList<>();
        List<GeoPoint> qPoints = new ArrayList<>();
        createQPoints(qHashes, qPoints);

        GeoDistanceSortBuilder geoDistanceSortBuilder = new GeoDistanceSortBuilder("location");
        for (int i = 0; i < 4; i++) {
            int at = randomInt(3 - i);
            if (randomBoolean()) {
                geoDistanceSortBuilder.geohashes(qHashes.get(at));
            } else {
                geoDistanceSortBuilder.points(qPoints.get(at));
            }
            qHashes.remove(at);
            qPoints.remove(at);
        }

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(geoDistanceSortBuilder.sortMode("min").order(SortOrder.ASC).geoDistance(GeoDistance.PLANE).unit(DistanceUnit.KILOMETERS))
                .execute().actionGet();
        assertOrderedSearchHits(searchResponse, "d1", "d2");
        assertThat((Double) searchResponse.getHits().getAt(0).getSortValues()[0], closeTo(GeoDistance.PLANE.calculate(2.5, 1, 2, 1, DistanceUnit.KILOMETERS), 1.e-4));
        assertThat((Double) searchResponse.getHits().getAt(1).getSortValues()[0], closeTo(GeoDistance.PLANE.calculate(4.5, 1, 2, 1, DistanceUnit.KILOMETERS), 1.e-4));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(geoDistanceSortBuilder.sortMode("max").order(SortOrder.ASC).geoDistance(GeoDistance.PLANE).unit(DistanceUnit.KILOMETERS))
                .execute().actionGet();
        assertOrderedSearchHits(searchResponse, "d1", "d2");
        assertThat((Double) searchResponse.getHits().getAt(0).getSortValues()[0], closeTo(GeoDistance.PLANE.calculate(3.25, 4, 2, 1, DistanceUnit.KILOMETERS), 1.e-4));
        assertThat((Double) searchResponse.getHits().getAt(1).getSortValues()[0], closeTo(GeoDistance.PLANE.calculate(5.25, 4, 2, 1, DistanceUnit.KILOMETERS), 1.e-4));

        //test all the different formats in one
        createQPoints(qHashes, qPoints);
        XContentBuilder searchSourceBuilder = jsonBuilder();
        searchSourceBuilder.startObject().startArray("sort").startObject().startObject("_geo_distance").startArray("location");

        for (int i = 0; i < 4; i++) {
            int at = randomInt(qPoints.size() - 1);
            int format = randomInt(3);
            switch (format) {
                case 0: {
                    searchSourceBuilder.value(qHashes.get(at));
                    break;
                }
                case 1: {
                    searchSourceBuilder.value(qPoints.get(at).lat() + "," + qPoints.get(at).lon());
                    break;
                }
                case 2: {
                    searchSourceBuilder.value(qPoints.get(at));
                    break;
                }
                case 3: {
                    searchSourceBuilder.startArray().value(qPoints.get(at).lon()).value(qPoints.get(at).lat()).endArray();
                    break;
                }
            }
            qHashes.remove(at);
            qPoints.remove(at);
        }

        searchSourceBuilder.endArray();
        searchSourceBuilder.field("order", "asc");
        searchSourceBuilder.field("unit", "km");
        searchSourceBuilder.field("sort_mode", "min");
        searchSourceBuilder.field("distance_type", "plane");
        searchSourceBuilder.endObject();
        searchSourceBuilder.endObject();
        searchSourceBuilder.endArray();
        searchSourceBuilder.endObject();

        searchResponse = client().prepareSearch().setSource(searchSourceBuilder.bytes()).execute().actionGet();
        assertOrderedSearchHits(searchResponse, "d1", "d2");
        assertThat((Double) searchResponse.getHits().getAt(0).getSortValues()[0], closeTo(GeoDistance.PLANE.calculate(2.5, 1, 2, 1, DistanceUnit.KILOMETERS), 1.e-4));
        assertThat((Double) searchResponse.getHits().getAt(1).getSortValues()[0], closeTo(GeoDistance.PLANE.calculate(4.5, 1, 2, 1, DistanceUnit.KILOMETERS), 1.e-4));
    }

    public void testSinglePointGeoDistanceSort() throws ExecutionException, InterruptedException, IOException {
        assertAcked(prepareCreate("index").addMapping("type", "location", "type=geo_point"));
        indexRandom(true,
                client().prepareIndex("index", "type", "d1").setSource(jsonBuilder().startObject().startObject("location").field("lat", 1).field("lon", 1).endObject().endObject()),
                client().prepareIndex("index", "type", "d2").setSource(jsonBuilder().startObject().startObject("location").field("lat", 1).field("lon", 2).endObject().endObject()));
        ensureYellow();

        String hashPoint = "s037ms06g7h0";

        GeoDistanceSortBuilder geoDistanceSortBuilder = new GeoDistanceSortBuilder("location");
        geoDistanceSortBuilder.geohashes(hashPoint);

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(geoDistanceSortBuilder.sortMode("min").order(SortOrder.ASC).geoDistance(GeoDistance.PLANE).unit(DistanceUnit.KILOMETERS))
                .execute().actionGet();
        checkCorrectSortOrderForGeoSort(searchResponse);

        geoDistanceSortBuilder = new GeoDistanceSortBuilder("location");
        geoDistanceSortBuilder.points(new GeoPoint(2, 2));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(geoDistanceSortBuilder.sortMode("min").order(SortOrder.ASC).geoDistance(GeoDistance.PLANE).unit(DistanceUnit.KILOMETERS))
                .execute().actionGet();
        checkCorrectSortOrderForGeoSort(searchResponse);

        geoDistanceSortBuilder = new GeoDistanceSortBuilder("location");
        geoDistanceSortBuilder.point(2, 2);

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(geoDistanceSortBuilder.sortMode("min").order(SortOrder.ASC).geoDistance(GeoDistance.PLANE).unit(DistanceUnit.KILOMETERS))
                .execute().actionGet();
        checkCorrectSortOrderForGeoSort(searchResponse);

        String geoSortRequest = jsonBuilder().startObject().startArray("sort").startObject()
                .startObject("_geo_distance")
                .startArray("location").value(2f).value(2f).endArray()
                .field("unit", "km")
                .field("distance_type", "plane")
                .endObject()
                .endObject().endArray().string();
        searchResponse = client().prepareSearch().setSource(new BytesArray(geoSortRequest))
                .execute().actionGet();
        checkCorrectSortOrderForGeoSort(searchResponse);

        geoSortRequest = jsonBuilder().startObject().startArray("sort").startObject()
                .startObject("_geo_distance")
                .field("location", "s037ms06g7h0")
                .field("unit", "km")
                .field("distance_type", "plane")
                .endObject()
                .endObject().endArray().string();
        searchResponse = client().prepareSearch().setSource(new BytesArray(geoSortRequest))
                .execute().actionGet();
        checkCorrectSortOrderForGeoSort(searchResponse);

        geoSortRequest = jsonBuilder().startObject().startArray("sort").startObject()
                .startObject("_geo_distance")
                .startObject("location")
                .field("lat", 2)
                .field("lon", 2)
                .endObject()
                .field("unit", "km")
                .field("distance_type", "plane")
                .endObject()
                .endObject().endArray().string();
        searchResponse = client().prepareSearch().setSource(new BytesArray(geoSortRequest))
                .execute().actionGet();
        checkCorrectSortOrderForGeoSort(searchResponse);
    }

    private void checkCorrectSortOrderForGeoSort(SearchResponse searchResponse) {
        assertOrderedSearchHits(searchResponse, "d2", "d1");
        assertThat((Double) searchResponse.getHits().getAt(0).getSortValues()[0], closeTo(GeoDistance.PLANE.calculate(2, 2, 1, 2, DistanceUnit.KILOMETERS), 1.e-4));
        assertThat((Double) searchResponse.getHits().getAt(1).getSortValues()[0], closeTo(GeoDistance.PLANE.calculate(2, 2, 1, 1, DistanceUnit.KILOMETERS), 1.e-4));
    }

    protected void createQPoints(List<String> qHashes, List<GeoPoint> qPoints) {
        GeoPoint[] qp = {new GeoPoint(2, 1), new GeoPoint(2, 2), new GeoPoint(2, 3), new GeoPoint(2, 4)};
        qPoints.addAll(Arrays.asList(qp));
        String[] qh = {"s02equ04ven0", "s037ms06g7h0", "s065kk0dc540", "s06g7h0dyg00"};
        qHashes.addAll(Arrays.asList(qh));
    }

    public void testCrossIndexIgnoreUnmapped() throws Exception {
        assertAcked(prepareCreate("test1").addMapping(
                "type", "str_field1", "type=string",
                "long_field", "type=long",
                "double_field", "type=double").get());
        assertAcked(prepareCreate("test2").get());

        indexRandom(true,
                client().prepareIndex("test1", "type").setSource("str_field", "bcd", "long_field", 3, "double_field", 0.65),
                client().prepareIndex("test2", "type").setSource());

        ensureYellow("test1", "test2");

        SearchResponse resp = client().prepareSearch("test1", "test2")
                .addSort(fieldSort("str_field").order(SortOrder.ASC).unmappedType("string"))
                .addSort(fieldSort("str_field2").order(SortOrder.DESC).unmappedType("string")).get();

        assertSortValues(resp,
                new Object[] {new StringAndBytesText("bcd"), null},
                new Object[] {null, null});

        resp = client().prepareSearch("test1", "test2")
                .addSort(fieldSort("long_field").order(SortOrder.ASC).unmappedType("long"))
                .addSort(fieldSort("long_field2").order(SortOrder.DESC).unmappedType("long")).get();
        assertSortValues(resp,
                new Object[] {3L, Long.MIN_VALUE},
                new Object[] {Long.MAX_VALUE, Long.MIN_VALUE});

        resp = client().prepareSearch("test1", "test2")
                .addSort(fieldSort("double_field").order(SortOrder.ASC).unmappedType("double"))
                .addSort(fieldSort("double_field2").order(SortOrder.DESC).unmappedType("double")).get();
        assertSortValues(resp,
                new Object[] {0.65, Double.NEGATIVE_INFINITY},
                new Object[] {Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY});
    }

}
