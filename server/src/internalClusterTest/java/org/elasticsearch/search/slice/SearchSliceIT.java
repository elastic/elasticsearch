/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.slice;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.sort.ShardDocSortField;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

public class SearchSliceIT extends ESIntegTestCase {
    private void setupIndex(int numDocs, int numberOfShards) throws IOException, ExecutionException, InterruptedException {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("invalid_random_kw")
                .field("type", "keyword")
                .field("doc_values", "false")
                .endObject()
                .startObject("random_int")
                .field("type", "integer")
                .field("doc_values", "true")
                .endObject()
                .startObject("invalid_random_int")
                .field("type", "integer")
                .field("doc_values", "false")
                .endObject()
                .endObject()
                .endObject()
        );
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setSettings(Settings.builder().put("number_of_shards", numberOfShards).put("index.max_slices_per_scroll", 10000))
                .setMapping(mapping)
        );
        ensureGreen();

        List<IndexRequestBuilder> requests = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            XContentBuilder builder = jsonBuilder().startObject()
                .field("invalid_random_kw", randomAlphaOfLengthBetween(5, 20))
                .field("random_int", randomInt())
                .field("static_int", 0)
                .field("invalid_random_int", randomInt())
                .endObject();
            requests.add(prepareIndex("test").setSource(builder));
        }
        indexRandom(true, requests);
    }

    public void testSearchSort() throws Exception {
        int numShards = randomIntBetween(1, 7);
        int numDocs = randomIntBetween(100, 1000);
        setupIndex(numDocs, numShards);
        int max = randomIntBetween(2, numShards * 3);
        for (String field : new String[] { "_id", "random_int", "static_int" }) {
            int fetchSize = randomIntBetween(10, 100);
            // test _doc sort
            SearchRequestBuilder request = prepareSearch("test").setQuery(matchAllQuery())
                .setScroll(new Scroll(TimeValue.timeValueSeconds(10)))
                .setSize(fetchSize)
                .addSort(SortBuilders.fieldSort("_doc"));
            assertSearchSlicesWithScroll(request, field, max, numDocs);

            // test numeric sort
            request = prepareSearch("test").setQuery(matchAllQuery())
                .setScroll(new Scroll(TimeValue.timeValueSeconds(10)))
                .addSort(SortBuilders.fieldSort("random_int"))
                .setSize(fetchSize);
            assertSearchSlicesWithScroll(request, field, max, numDocs);
        }
    }

    public void testWithPreferenceAndRoutings() throws Exception {
        int numShards = 10;
        int totalDocs = randomIntBetween(100, 1000);
        setupIndex(totalDocs, numShards);

        assertResponse(prepareSearch("test").setQuery(matchAllQuery()).setPreference("_shards:1,4").setSize(0), sr -> {
            int numDocs = (int) sr.getHits().getTotalHits().value();
            int max = randomIntBetween(2, numShards * 3);
            int fetchSize = randomIntBetween(10, 100);
            SearchRequestBuilder request = prepareSearch("test").setQuery(matchAllQuery())
                .setScroll(new Scroll(TimeValue.timeValueSeconds(10)))
                .setSize(fetchSize)
                .setPreference("_shards:1,4")
                .addSort(SortBuilders.fieldSort("_doc"));
            assertSearchSlicesWithScroll(request, "_id", max, numDocs);
        });

        assertResponse(prepareSearch("test").setQuery(matchAllQuery()).setRouting("foo", "bar").setSize(0), sr -> {
            int numDocs = (int) sr.getHits().getTotalHits().value();
            int max = randomIntBetween(2, numShards * 3);
            int fetchSize = randomIntBetween(10, 100);
            SearchRequestBuilder request = prepareSearch("test").setQuery(matchAllQuery())
                .setScroll(new Scroll(TimeValue.timeValueSeconds(10)))
                .setSize(fetchSize)
                .setRouting("foo", "bar")
                .addSort(SortBuilders.fieldSort("_doc"));
            assertSearchSlicesWithScroll(request, "_id", max, numDocs);
        });

        assertAcked(
            indicesAdmin().prepareAliases()
                .addAliasAction(IndicesAliasesRequest.AliasActions.add().index("test").alias("alias1").routing("foo"))
                .addAliasAction(IndicesAliasesRequest.AliasActions.add().index("test").alias("alias2").routing("bar"))
                .addAliasAction(IndicesAliasesRequest.AliasActions.add().index("test").alias("alias3").routing("baz"))
        );
        assertResponse(prepareSearch("alias1", "alias3").setQuery(matchAllQuery()).setSize(0), sr -> {
            int numDocs = (int) sr.getHits().getTotalHits().value();
            int max = randomIntBetween(2, numShards * 3);
            int fetchSize = randomIntBetween(10, 100);
            SearchRequestBuilder request = prepareSearch("alias1", "alias3").setQuery(matchAllQuery())
                .setScroll(new Scroll(TimeValue.timeValueSeconds(10)))
                .setSize(fetchSize)
                .addSort(SortBuilders.fieldSort("_doc"));
            assertSearchSlicesWithScroll(request, "_id", max, numDocs);
        });
    }

    private void assertSearchSlicesWithScroll(SearchRequestBuilder request, String field, int numSlice, int numDocs) {
        int totalResults = 0;
        List<String> keys = new ArrayList<>();
        for (int id = 0; id < numSlice; id++) {
            SliceBuilder sliceBuilder = new SliceBuilder(field, id, numSlice);
            SearchResponse searchResponse = request.slice(sliceBuilder).get();
            try {
                totalResults += searchResponse.getHits().getHits().length;
                int expectedSliceResults = (int) searchResponse.getHits().getTotalHits().value();
                int numSliceResults = searchResponse.getHits().getHits().length;
                String scrollId = searchResponse.getScrollId();
                for (SearchHit hit : searchResponse.getHits().getHits()) {
                    assertTrue(keys.add(hit.getId()));
                }
                while (searchResponse.getHits().getHits().length > 0) {
                    searchResponse.decRef();
                    searchResponse = client().prepareSearchScroll("test")
                        .setScrollId(scrollId)
                        .setScroll(new Scroll(TimeValue.timeValueSeconds(10)))
                        .get();
                    scrollId = searchResponse.getScrollId();
                    totalResults += searchResponse.getHits().getHits().length;
                    numSliceResults += searchResponse.getHits().getHits().length;
                    for (SearchHit hit : searchResponse.getHits().getHits()) {
                        assertTrue(keys.add(hit.getId()));
                    }
                }
                assertThat(numSliceResults, equalTo(expectedSliceResults));
                clearScroll(scrollId);
            } finally {
                searchResponse.decRef();
            }
        }
        assertThat(totalResults, equalTo(numDocs));
        assertThat(keys.size(), equalTo(numDocs));
        assertThat(new HashSet<>(keys).size(), equalTo(numDocs));
    }

    public void testPointInTime() throws Exception {
        int numShards = randomIntBetween(1, 7);
        int numDocs = randomIntBetween(100, 1000);
        setupIndex(numDocs, numShards);
        int max = randomIntBetween(2, numShards * 3);

        // Test the default slicing strategy (null), as well as numeric doc values
        for (String field : new String[] { null, "random_int", "static_int" }) {
            // Open point-in-time reader
            OpenPointInTimeRequest request = new OpenPointInTimeRequest("test").keepAlive(TimeValue.timeValueSeconds(10));
            OpenPointInTimeResponse response = client().execute(TransportOpenPointInTimeAction.TYPE, request).actionGet();
            BytesReference pointInTimeId = response.getPointInTimeId();

            // Test sort on document IDs
            assertSearchSlicesWithPointInTime(field, ShardDocSortField.NAME, pointInTimeId, max, numDocs);
            // Test numeric sort
            assertSearchSlicesWithPointInTime(field, "random_int", pointInTimeId, max, numDocs);

            // Close point-in-time reader
            client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pointInTimeId)).actionGet();
        }
    }

    private void assertSearchSlicesWithPointInTime(
        String sliceField,
        String sortField,
        BytesReference pointInTimeId,
        int numSlice,
        int numDocs
    ) {
        int totalResults = 0;
        List<String> keys = new ArrayList<>();
        for (int id = 0; id < numSlice; id++) {
            int numSliceResults = 0;

            SearchRequestBuilder request = prepareSearch().slice(new SliceBuilder(sliceField, id, numSlice))
                .setPointInTime(new PointInTimeBuilder(pointInTimeId))
                .addSort(SortBuilders.fieldSort(sortField))
                .setSize(randomIntBetween(10, 100));

            SearchResponse searchResponse = request.get();
            try {
                int expectedSliceResults = (int) searchResponse.getHits().getTotalHits().value();

                while (true) {
                    int numHits = searchResponse.getHits().getHits().length;
                    if (numHits == 0) {
                        break;
                    }

                    totalResults += numHits;
                    numSliceResults += numHits;
                    for (SearchHit hit : searchResponse.getHits().getHits()) {
                        assertTrue(keys.add(hit.getId()));
                    }

                    Object[] sortValues = searchResponse.getHits().getHits()[numHits - 1].getSortValues();
                    searchResponse.decRef();
                    searchResponse = request.searchAfter(sortValues).get();
                }
                assertThat(numSliceResults, equalTo(expectedSliceResults));
            } finally {
                searchResponse.decRef();
            }
        }
        assertThat(totalResults, equalTo(numDocs));
        assertThat(keys.size(), equalTo(numDocs));
        assertThat(new HashSet<>(keys).size(), equalTo(numDocs));
    }

    public void testInvalidFields() throws Exception {
        setupIndex(0, 1);
        SearchPhaseExecutionException exc = expectThrows(
            SearchPhaseExecutionException.class,
            prepareSearch("test").setQuery(matchAllQuery())
                .setScroll(new Scroll(TimeValue.timeValueSeconds(10)))
                .slice(new SliceBuilder("invalid_random_int", 0, 10))

        );
        Throwable rootCause = findRootCause(exc);
        assertThat(rootCause.getClass(), equalTo(IllegalArgumentException.class));
        assertThat(rootCause.getMessage(), startsWith("cannot load numeric doc values"));

        exc = expectThrows(
            SearchPhaseExecutionException.class,
            prepareSearch("test").setQuery(matchAllQuery())
                .setScroll(new Scroll(TimeValue.timeValueSeconds(10)))
                .slice(new SliceBuilder("invalid_random_kw", 0, 10))
        );
        rootCause = findRootCause(exc);
        assertThat(rootCause.getClass(), equalTo(IllegalArgumentException.class));
        assertThat(rootCause.getMessage(), startsWith("cannot load numeric doc values"));
    }

    public void testInvalidQuery() throws Exception {
        setupIndex(0, 1);
        ActionRequestValidationException exc = expectThrows(
            ActionRequestValidationException.class,
            prepareSearch().setQuery(matchAllQuery()).slice(new SliceBuilder("invalid_random_int", 0, 10))
        );
        assertThat(exc.getMessage(), containsString("[slice] can only be used with [scroll] or [point-in-time] requests"));
    }

    private Throwable findRootCause(Exception e) {
        Throwable ret = e;
        while (ret.getCause() != null) {
            ret = ret.getCause();
        }
        return ret;
    }
}
