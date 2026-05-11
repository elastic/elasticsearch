/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.reindex.AbstractBulkByPaginatedSearchRequest;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.ReindexRequestBuilder;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.search.internal.SearchContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

public class ReindexBasicTests extends ReindexTestCase {
    public void testFiltering() throws Exception {
        indexRandom(
            true,
            prepareIndex("source").setId("1").setSource("foo", "a"),
            prepareIndex("source").setId("2").setSource("foo", "a"),
            prepareIndex("source").setId("3").setSource("foo", "b"),
            prepareIndex("source").setId("4").setSource("foo", "c")
        );
        assertHitCount(prepareSearch("source").setSize(0), 4);

        // Copy all the docs
        ReindexRequestBuilder copy = reindex().source("source").destination("dest").refresh(true);
        assertThat(copy.get(), matcher().created(4));
        assertHitCount(prepareSearch("dest").setSize(0), 4);

        // Now none of them
        createIndex("none");
        copy = reindex().source("source").destination("none").filter(termQuery("foo", "no_match")).refresh(true);
        assertThat(copy.get(), matcher().created(0));
        assertHitCount(prepareSearch("none").setSize(0), 0);

        // Now half of them
        copy = reindex().source("source").destination("dest_half").filter(termQuery("foo", "a")).refresh(true);
        assertThat(copy.get(), matcher().created(2));
        assertHitCount(prepareSearch("dest_half").setSize(0), 2);

        // Limit with maxDocs
        copy = reindex().source("source").destination("dest_size_one").maxDocs(1).refresh(true);
        assertThat(copy.get(), matcher().created(1));
        assertHitCount(prepareSearch("dest_size_one").setSize(0), 1);
    }

    public void testCopyMany() throws Exception {
        List<IndexRequestBuilder> docs = new ArrayList<>();
        int max = between(150, 500);
        for (int i = 0; i < max; i++) {
            docs.add(prepareIndex("source").setId(Integer.toString(i)).setSource("foo", "a"));
        }

        indexRandom(true, docs);
        assertHitCount(prepareSearch("source").setSize(0), max);

        // Copy all the docs
        ReindexRequestBuilder copy = reindex().source("source").destination("dest").refresh(true);
        // Use a small batch size so we have to use more than one batch
        copy.source().setSize(5);
        assertThat(copy.get(), matcher().created(max).total(max).batches(max, 5));
        assertHitCount(prepareSearch("dest").setSize(0), max);

        // Copy some of the docs
        int half = max / 2;
        copy = reindex().source("source").destination("dest_half").refresh(true);
        // Use a small batch size so we have to use more than one batch
        copy.source().setSize(5);
        copy.maxDocs(half);
        assertThat(copy.get(), matcher().created(half).total(half).batches(half, 5));
        assertHitCount(prepareSearch("dest_half").setSize(0), half);
    }

    public void testTotalIsAccurateWhenSourceExceedsDefaultTrackTotalHits() {
        int numDocs = SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO + randomIntBetween(1, SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO);

        indexRandom(true, "source", numDocs);
        // The verification searches must opt into accurate total tracking themselves; otherwise they hit the same default
        // we are testing reindex's behaviour around.
        assertHitCount(prepareSearch("source").setTrackTotalHits(true).setSize(0), numDocs);

        BulkByScrollResponse response = reindex().source("source").destination("dest").refresh(true).get();
        assertThat(response, matcher().created(numDocs).total(numDocs));
        assertHitCount(prepareSearch("dest").setTrackTotalHits(true).setSize(0), numDocs);
    }

    /**
     * Reindex with source and destination specified as index aliases
     */
    public void testReindexWithAliases() {
        String prefix = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        String srcIndex = prefix + "_src_idx";
        String destIndex = prefix + "_dest_idx";
        String srcAlias = prefix + "_src_alias";
        String destAlias = prefix + "_dest_alias";

        assertAcked(prepareCreate(srcIndex).addAlias(new Alias(srcAlias)));
        assertAcked(prepareCreate(destIndex).addAlias(new Alias(destAlias)));
        ensureGreen(srcIndex, destIndex);

        int numDocs = between(5, 50);
        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            docs.add(prepareIndex(srcIndex).setId(Integer.toString(i)).setSource("foo", "bar"));
        }
        indexRandom(true, docs);
        assertHitCount(prepareSearch(srcAlias).setSize(0), numDocs);

        ReindexRequestBuilder copy = reindex().source(srcAlias).destination(destAlias).refresh(true);
        assertThat(copy.get(), matcher().created(numDocs));

        assertHitCount(prepareSearch(destAlias).setSize(0), numDocs);
        assertHitCount(prepareSearch(destIndex).setSize(0), numDocs);
    }

    /**
     * Tests that high throttling (that would exceed the scroll keep-alive) does not terminate a reindexing operation early.
     * This is achieved by setting the scroll keep-alive to 500ms second, and the throttling is (5 docs at 4 req/s ≈ 1.25s)
     */
    public void testSmallScrollTimeoutWithHeavyThrottleCompletes() {
        String prefix = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        String source = prefix + "src";
        String dest = prefix + "dest";

        assertAcked(prepareCreate(source));
        assertAcked(prepareCreate(dest));
        ensureGreen(source, dest);

        int numDocs = 10;
        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            docs.add(prepareIndex(source).setId(Integer.toString(i)).setSource("foo", "a"));
        }
        indexRandom(true, docs);
        assertHitCount(prepareSearch(source).setSize(0), numDocs);

        ReindexRequestBuilder copy = reindex().source(source).destination(dest).refresh(true).setSlices(1);
        copy.source().setScroll(TimeValue.timeValueMillis(500));
        copy.source().setSize(5);
        copy.setRequestsPerSecond(4.0f);

        BulkByScrollResponse response = copy.get();
        assertThat(response, matcher().created(numDocs));
        assertThat(response.getSearchFailures(), empty());
        assertHitCount(prepareSearch(dest).setSize(0), numDocs);
    }

    /**
     * Copy many documents with {@code slices=auto}, then a second reindex with {@code maxDocs}.
     */
    public void testCopyManyWithAutoSlicing() {
        String prefix = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        String source = prefix + "_src";
        String dest = prefix + "_dest";
        String destHalf = prefix + "_dest_half";
        int primaries = rarely() ? randomIntBetween(21, 25) : randomIntBetween(1, 10);
        assertAcked(prepareCreate(source).setSettings(Settings.builder().put("number_of_shards", primaries)));
        ensureGreen(source);

        List<IndexRequestBuilder> docs = new ArrayList<>();
        int max = between(150, 500);
        for (int i = 0; i < max; i++) {
            docs.add(prepareIndex(source).setId(Integer.toString(i)).setSource("foo", "a"));
        }

        indexRandom(true, docs);
        assertHitCount(prepareSearch(source).setSize(0), max);

        int expectedStatuses = expectedSliceStatuses(AbstractBulkByPaginatedSearchRequest.AUTO_SLICES, source);

        ReindexRequestBuilder copy = reindex().source(source)
            .destination(dest)
            .refresh(true)
            .setSlices(AbstractBulkByPaginatedSearchRequest.AUTO_SLICES);
        copy.source().setSize(5);
        assertThat(copy.get(), matcher().created(max).batches(greaterThanOrEqualTo(max / 5)).slices(hasSize(expectedStatuses)));
        assertHitCount(prepareSearch(dest).setSize(0), max);

        int half = max / 2;
        copy = reindex().source(source).destination(destHalf).refresh(true).setSlices(AbstractBulkByPaginatedSearchRequest.AUTO_SLICES);
        copy.source().setSize(5);
        copy.maxDocs(half);
        BulkByScrollResponse response = copy.get();
        assertThat(response, matcher().created(lessThanOrEqualTo((long) half)).slices(hasSize(expectedStatuses)));
        assertHitCount(prepareSearch(destHalf).setSize(0), response.getCreated());
    }

    /**
     * Copy many documents with an explicit {@code slices} count, then a second reindex with {@code maxDocs}
     */
    public void testCopyManyWithFixedSlicing() {
        String prefix = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        String source = prefix + "_src";
        String dest = prefix + "_dest";
        String destHalf = prefix + "_dest_half";
        int primaries = randomIntBetween(1, 3);
        assertAcked(prepareCreate(source).setSettings(Settings.builder().put("number_of_shards", primaries)));
        ensureGreen(source);

        int slices = randomIntBetween(1, 10);

        List<IndexRequestBuilder> docs = new ArrayList<>();
        int max = between(150, 500);
        for (int i = 0; i < max; i++) {
            docs.add(prepareIndex(source).setId(Integer.toString(i)).setSource("foo", "a"));
        }

        indexRandom(true, docs);
        assertHitCount(prepareSearch(source).setSize(0), max);

        int expectedStatuses = expectedSliceStatuses(slices, source);

        ReindexRequestBuilder copy = reindex().source(source).destination(dest).refresh(true).setSlices(slices);
        copy.source().setSize(5);
        assertThat(copy.get(), matcher().created(max).batches(greaterThanOrEqualTo(max / 5)).slices(hasSize(expectedStatuses)));
        assertHitCount(prepareSearch(dest).setSize(0), max);

        int half = max / 2;
        copy = reindex().source(source).destination(destHalf).refresh(true).setSlices(slices);
        copy.source().setSize(5);
        copy.maxDocs(half);
        BulkByScrollResponse response = copy.get();
        assertThat(response, matcher().created(lessThanOrEqualTo((long) half)).slices(hasSize(expectedStatuses)));
        assertHitCount(prepareSearch(destHalf).setSize(0), response.getCreated());
    }

    /**
     * Reindex from several sources with {@code slices=auto}. Effective slice count is {@code min(primaries per index)} capped at
     * {@link org.elasticsearch.reindex.BulkByPaginatedSearchParallelizationHelper#AUTO_SLICE_CEILING}.
     */
    public void testMultipleSourcesWithAutoSlicing() {
        String prefix = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        int numSources = between(2, 5);

        Map<String, List<IndexRequestBuilder>> docs = new HashMap<>();
        for (int i = 0; i < numSources; i++) {
            String indexName = prefix + "_src" + i;
            // Rarely have the number of primaries be >20 so that we're testing we cap at AUTO_SLICE_CEILING
            int primaries = rarely() ? randomIntBetween(21, 25) : randomIntBetween(1, 10);
            assertAcked(prepareCreate(indexName).setSettings(Settings.builder().put("number_of_shards", primaries)));
            docs.put(indexName, new ArrayList<>());
            int numDocs = between(50, 200);
            for (int d = 0; d < numDocs; d++) {
                docs.get(indexName).add(prepareIndex(indexName).setId("id_" + i + "_" + d).setSource("foo", "a"));
            }
        }
        ensureGreen(docs.keySet().toArray(new String[0]));

        List<IndexRequestBuilder> allDocs = docs.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
        indexRandom(true, allDocs);
        for (Map.Entry<String, List<IndexRequestBuilder>> entry : docs.entrySet()) {
            assertHitCount(prepareSearch(entry.getKey()).setSize(0), entry.getValue().size());
        }

        int expectedStatuses = expectedSliceStatuses(AbstractBulkByPaginatedSearchRequest.AUTO_SLICES, docs.keySet());

        String dest = prefix + "_dest";
        String[] sourceNames = docs.keySet().toArray(new String[0]);
        ReindexRequestBuilder request = reindex().source(sourceNames)
            .destination(dest)
            .refresh(true)
            .setSlices(AbstractBulkByPaginatedSearchRequest.AUTO_SLICES);
        request.source().setSize(5);

        BulkByScrollResponse response = request.get();
        int totalDocs = allDocs.size();
        assertThat(response, matcher().created(totalDocs).batches(greaterThanOrEqualTo(totalDocs / 5)).slices(hasSize(expectedStatuses)));
        assertHitCount(prepareSearch(dest).setSize(0), totalDocs);
    }

    /**
     * Reindex from several sources with an explicit {@code slices} count. Primaries per index are random so runs
     * compare fixed slice counts to varied shard layouts.
     */
    public void testMultipleSourcesWithFixedSlicing() {
        String prefix = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        int numSources = between(2, 5);
        int slices = randomIntBetween(1, 10);

        Map<String, List<IndexRequestBuilder>> docs = new HashMap<>();
        for (int i = 0; i < numSources; i++) {
            String indexName = prefix + "_src" + i;
            int primaries = randomIntBetween(1, 3);
            assertAcked(prepareCreate(indexName).setSettings(Settings.builder().put("number_of_shards", primaries)));
            docs.put(indexName, new ArrayList<>());
            int numDocs = between(50, 200);
            for (int d = 0; d < numDocs; d++) {
                docs.get(indexName).add(prepareIndex(indexName).setId("id_" + i + "_" + d).setSource("foo", "a"));
            }
        }
        ensureGreen(docs.keySet().toArray(new String[0]));

        List<IndexRequestBuilder> allDocs = docs.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
        indexRandom(true, allDocs);
        for (Map.Entry<String, List<IndexRequestBuilder>> entry : docs.entrySet()) {
            assertHitCount(prepareSearch(entry.getKey()).setSize(0), entry.getValue().size());
        }

        int expectedStatuses = expectedSliceStatuses(slices, docs.keySet());

        String dest = prefix + "_dest";
        String[] sourceNames = docs.keySet().toArray(new String[0]);
        ReindexRequestBuilder request = reindex().source(sourceNames).destination(dest).refresh(true).setSlices(slices);
        request.source().setSize(5);

        BulkByScrollResponse response = request.get();
        int totalDocs = allDocs.size();
        assertThat(response, matcher().created(totalDocs).batches(greaterThanOrEqualTo(totalDocs / 5)).slices(hasSize(expectedStatuses)));
        assertHitCount(prepareSearch(dest).setSize(0), totalDocs);
    }

    public void testMissingSources() {
        BulkByScrollResponse response = updateByQuery().source("missing-index-*")
            .refresh(true)
            .setSlices(AbstractBulkByPaginatedSearchRequest.AUTO_SLICES)
            .get();
        assertThat(response, matcher().created(0).slices(hasSize(0)));
    }

    public void testReindexFromComplexDateMathIndexName() throws Exception {
        String sourceIndexName = "datemath-2001-01-01-14";
        String destIndexName = "<reindex-datemath-{2001-01-01-13||+1h/h{yyyy-MM-dd-HH|-07:00}}>";
        indexRandom(
            true,
            prepareIndex(sourceIndexName).setId("1").setSource("foo", "a"),
            prepareIndex(sourceIndexName).setId("2").setSource("foo", "a"),
            prepareIndex(sourceIndexName).setId("3").setSource("foo", "b"),
            prepareIndex(sourceIndexName).setId("4").setSource("foo", "c")
        );
        assertHitCount(prepareSearch(sourceIndexName).setSize(0), 4);

        // Copy all the docs
        ReindexRequestBuilder copy = reindex().source(sourceIndexName).destination(destIndexName).refresh(true);
        assertThat(copy.get(), matcher().created(4));
        assertHitCount(prepareSearch(destIndexName).setSize(0), 4);
    }

    public void testReindexIncludeVectors() throws Exception {
        var resp1 = prepareCreate("test").setSettings(
            Settings.builder().put(IndexSettings.INDEX_MAPPING_EXCLUDE_SOURCE_VECTORS_SETTING.getKey(), true).build()
        ).setMapping("foo", "type=dense_vector,similarity=l2_norm", "bar", "type=sparse_vector").get();
        assertAcked(resp1);

        var resp2 = prepareCreate("test_reindex").setSettings(
            Settings.builder().put(IndexSettings.INDEX_MAPPING_EXCLUDE_SOURCE_VECTORS_SETTING.getKey(), true).build()
        ).setMapping("foo", "type=dense_vector,similarity=l2_norm", "bar", "type=sparse_vector").get();
        assertAcked(resp2);

        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource("foo", List.of(3f, 2f, 1.5f), "bar", Map.of("token_1", 4f, "token_2", 7f))
        );

        var searchResponse = prepareSearch("test").get();
        try {
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(1L));
            assertThat(searchResponse.getHits().getHits().length, equalTo(1));
            var sourceMap = searchResponse.getHits().getAt(0).getSourceAsMap();
            assertThat(sourceMap.size(), equalTo(0));
        } finally {
            searchResponse.decRef();
        }

        // Copy all the docs
        ReindexRequestBuilder copy = reindex().source("test").destination("test_reindex").refresh(true);
        var reindexResponse = copy.get();
        assertThat(reindexResponse, matcher().created(1));

        searchResponse = prepareSearch("test_reindex").get();
        try {
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(1L));
            assertThat(searchResponse.getHits().getHits().length, equalTo(1));
            var sourceMap = searchResponse.getHits().getAt(0).getSourceAsMap();
            assertThat(sourceMap.size(), equalTo(0));
        } finally {
            searchResponse.decRef();
        }

        searchResponse = prepareSearch("test_reindex").setExcludeVectors(false).get();
        try {
            assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(1L));
            assertThat(searchResponse.getHits().getHits().length, equalTo(1));
            var sourceMap = searchResponse.getHits().getAt(0).getSourceAsMap();
            assertThat(sourceMap.get("foo"), anyOf(equalTo(List.of(3f, 2f, 1.5f)), equalTo(List.of(3d, 2d, 1.5d))));
            assertThat(
                sourceMap.get("bar"),
                anyOf(equalTo(Map.of("token_1", 4f, "token_2", 7f)), equalTo(Map.of("token_1", 4d, "token_2", 7d)))
            );
        } finally {
            searchResponse.decRef();
        }
    }

    /**
     * Reindex should fail when the source index is closed. By default, IndicesOptions forbids closed
     * indices, so the request fails at validation with IndexClosedException.
     */
    public void testReindexFailsWhenSourceIndexIsClosed() {
        String sourceIndex = "source";
        String destIndex = "dest";
        createIndex(sourceIndex);
        indexRandom(true, prepareIndex(sourceIndex).setId("1").setSource("foo", "bar"));
        indicesAdmin().prepareClose(sourceIndex).get();

        Exception e = expectThrows(Exception.class, () -> {
            ReindexRequest request = new ReindexRequest();
            request.setSourceIndices(sourceIndex);
            request.setDestIndex(destIndex);
            client().execute(ReindexAction.INSTANCE, request).actionGet();
        });

        assertThat(ExceptionsHelper.unwrap(e, IndexClosedException.class), notNullValue());
    }
}
