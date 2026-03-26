/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.management;

import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.rest.root.MainRestPlugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;

/**
 * Integration tests for <em>manual</em> reindex slicing.
 * <p>
 * The client issues separate {@link ReindexRequest}s, each specifying the
 * {@link SearchSourceBuilder#slice(SliceBuilder) slice} on the search body rather than
 * relying on the {@code slices} URL parameter to fan out parallel workers in one request.
 * Each slice restricts the search to a partition of matching documents. Running slice
 * {@code 0..max-1} in separate reindex calls eventually covers the same document set as a
 * single unsliced reindex, as long as the source query and slice parameters are consistent
 * across calls.
 * <p>
 * What we're testing here specifically is that source updates (e.g bulk updates) between
 * manual slices are handled correctly. The logical Elasticsearch {@code _id} for a document is
 * unchanged by an update-in-place, but internal representation (e.g. Lucene id / version) can
 * change. Therefore, by setting {@code dest.op_type = create}, we assert that each destination id
 * must be created exactly once across slices and duplicate indexing of the same id surfaces as version
 * conflicts in the {@link BulkByScrollResponse}.
 * Assertions check for zero version conflicts and empty bulk failures.
 *
 * As a note, when the user does not specify the slice field, we default to the stable document `_id` field

 * @see org.elasticsearch.reindex.Reindexer#initTask
 * @see SliceBuilder
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0)
public class ManualSlicingReindexIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ReindexPlugin.class, ReindexManagementPlugin.class, MainRestPlugin.class);
    }

    /**
     * We have three slices, with the source index updating between each one. We expect each document to
     * only be indexed once. As a note, we make no guarantees as to the value of the re-indexed document.
     * Ie, in this case, the first 10 documents will match the original source, then the next slice will
     * reindex documents that have been modified by the first bulk update, and the final slice will reindex
     * documents that have been updated a second time. At no point are we reindexing the <i>same</i> document
     */
    public void testReindexWithSourceUpdatesBetweenSlices() {
        assumeTrue("Need PIT", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);
        int docCount = randomIntBetween(500, 1000);
        String sourceIndex = randomAlphanumericOfLength(randomIntBetween(3, 5)).toLowerCase(Locale.ROOT);
        String destIndex = randomAlphanumericOfLength(randomIntBetween(3, 5)).toLowerCase(Locale.ROOT);

        createIndex(sourceIndex, randomIntBetween(1,3), randomIntBetween(0,1));
        indexRandom(
            true,
            IntStream.range(0, docCount)
                .mapToObj(i -> prepareIndex(sourceIndex).setId(String.valueOf(i)).setSource("foo", "v" + i))
                .toArray(IndexRequestBuilder[]::new)
        );
        assertHitCount(prepareSearch(sourceIndex).setSize(0), docCount);

        // Reindex slice 0
        ReindexRequest slice0Request = new ReindexRequest().setSourceIndices(sourceIndex).setDestIndex(destIndex).setRefresh(true).setDestOpType("create");
        slice0Request.getSearchRequest().source(new SearchSourceBuilder().slice(new SliceBuilder(0, 3)));
        BulkByScrollResponse slice0Response = client().execute(ReindexAction.INSTANCE, slice0Request).actionGet();
        assertNoReindexConflicts("slice 0", slice0Response);
        assertTrue("slice 0 should index some docs", slice0Response.getCreated() > 0);

        // Update all docs in source
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < docCount; i++) {
            bulkRequest.add(prepareIndex(sourceIndex).setId(String.valueOf(i)).setSource("foo", "v" + i, "updated", true).request());
        }
        client().bulk(bulkRequest).actionGet();
        client().admin().indices().refresh(new RefreshRequest(sourceIndex)).actionGet();

        // Reindex slice 1
        ReindexRequest slice1Request = new ReindexRequest().setSourceIndices(sourceIndex).setDestIndex(destIndex).setRefresh(true).setDestOpType("create");
        slice1Request.getSearchRequest().source(new SearchSourceBuilder().slice(new SliceBuilder(1, 3)));
        BulkByScrollResponse slice1Response = client().execute(ReindexAction.INSTANCE, slice1Request).actionGet();
        assertNoReindexConflicts("slice 1", slice1Response);
        assertTrue("slice 1 should index some docs", slice1Response.getCreated() > 0);

        // Update all docs in source
        BulkRequest bulkRequest2 = new BulkRequest();
        for (int i = 0; i < docCount; i++) {
            bulkRequest2.add(prepareIndex(sourceIndex).setId(String.valueOf(i)).setSource("foo", "v" + i, "updated again", true).request());
        }
        client().bulk(bulkRequest2).actionGet();
        client().admin().indices().refresh(new RefreshRequest(sourceIndex)).actionGet();

        // Reindex slice 2
        ReindexRequest slice2Request = new ReindexRequest().setSourceIndices(sourceIndex).setDestIndex(destIndex).setRefresh(true).setDestOpType("create");
        slice2Request.getSearchRequest().source(new SearchSourceBuilder().slice(new SliceBuilder(2, 3)));
        BulkByScrollResponse slice2Response = client().execute(ReindexAction.INSTANCE, slice2Request).actionGet();
        assertNoReindexConflicts("slice 2", slice2Response);
        assertTrue("slice 2 should index some docs", slice2Response.getCreated() > 0);

        assertDestinationHasAllDocIds("destination", destIndex, docCount);
    }

    /**
     * Tests manual reindexing into two destinations, with a source bulk update between slices.
     * Similar to {@link #testReindexWithSourceUpdatesBetweenSlices}, we check that updates to the source
     * index result in each document only being reindexed once. However, by inverting the reindexing order,
     * we ensure that even if the underlying implementation changes to use a different slice field reliant on
     * monotonically increasing ids, we'd still catch the regression.
     */
    public void testReindexIntoMultipleDestinationsWithSourceUpdatesBetweenSlices() {
        assumeTrue("Need PIT", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);
        int docCount = randomIntBetween(500,100);
        String sourceIndex = randomAlphanumericOfLength(randomIntBetween(3, 5)).toLowerCase(Locale.ROOT);
        String dest1 = randomAlphanumericOfLength(randomIntBetween(3, 5)).toLowerCase(Locale.ROOT);
        String dest2 = randomAlphanumericOfLength(randomIntBetween(3, 5)).toLowerCase(Locale.ROOT);

        createIndex(sourceIndex, 1, 0);
        indexRandom(
            true,
            IntStream.range(0, docCount)
                .mapToObj(i -> prepareIndex(sourceIndex).setId(String.valueOf(i)).setSource("foo", "v" + i))
                .toArray(IndexRequestBuilder[]::new)
        );
        assertHitCount(prepareSearch(sourceIndex).setSize(0), docCount);

        ReindexRequest slice0ToDest1 = new ReindexRequest().setSourceIndices(sourceIndex)
            .setDestIndex(dest1)
            .setRefresh(true)
            .setDestOpType("create");
        slice0ToDest1.getSearchRequest().source(new SearchSourceBuilder().slice(new SliceBuilder(0, 2)));
        BulkByScrollResponse slice0ToDest1Response = client().execute(ReindexAction.INSTANCE, slice0ToDest1).actionGet();
        assertNoReindexConflicts("slice 0 to dest1", slice0ToDest1Response);
        assertTrue("slice 0 to dest1 should index some docs", slice0ToDest1Response.getCreated() > 0);

        ReindexRequest slice1ToDest2 = new ReindexRequest().setSourceIndices(sourceIndex)
            .setDestIndex(dest2)
            .setRefresh(true)
            .setDestOpType("create");
        slice1ToDest2.getSearchRequest().source(new SearchSourceBuilder().slice(new SliceBuilder(1, 2)));
        BulkByScrollResponse slice1ToDest2Response = client().execute(ReindexAction.INSTANCE, slice1ToDest2).actionGet();
        assertNoReindexConflicts("slice 1 to dest2", slice1ToDest2Response);
        assertTrue("slice 1 to dest2 should index some docs", slice1ToDest2Response.getCreated() > 0);

        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < docCount; i++) {
            bulkRequest.add(prepareIndex(sourceIndex).setId(String.valueOf(i)).setSource("foo", "v" + i, "updated", true).request());
        }
        client().bulk(bulkRequest).actionGet();
        client().admin().indices().refresh(new RefreshRequest(sourceIndex)).actionGet();
        client().admin().indices().prepareForceMerge(sourceIndex).setMaxNumSegments(1).setFlush(true).get();

        ReindexRequest slice1ToDest1 = new ReindexRequest().setSourceIndices(sourceIndex)
            .setDestIndex(dest1)
            .setRefresh(true)
            .setDestOpType("create");
        slice1ToDest1.getSearchRequest().source(new SearchSourceBuilder().slice(new SliceBuilder(1, 2)));
        BulkByScrollResponse slice1ToDest1Response = client().execute(ReindexAction.INSTANCE, slice1ToDest1).actionGet();
        assertNoReindexConflicts("slice 1 to dest1 after bulk", slice1ToDest1Response);
        assertTrue("slice 1 to dest1 should index some docs", slice1ToDest1Response.getCreated() > 0);

        ReindexRequest slice0ToDest2 = new ReindexRequest().setSourceIndices(sourceIndex)
            .setDestIndex(dest2)
            .setRefresh(true)
            .setDestOpType("create");
        slice0ToDest2.getSearchRequest().source(new SearchSourceBuilder().slice(new SliceBuilder(0, 2)));
        BulkByScrollResponse slice0ToDest2Response = client().execute(ReindexAction.INSTANCE, slice0ToDest2).actionGet();
        assertNoReindexConflicts("slice 0 to dest2 after bulk", slice0ToDest2Response);
        assertTrue("slice 0 to dest2 should index some docs", slice0ToDest2Response.getCreated() > 0);

        assertDestinationHasAllDocIds("dest1", dest1, docCount);
        assertDestinationHasAllDocIds("dest2", dest2, docCount);
    }

    /**
     * Asserts that a reindex sub-request completed without indexing failures caused by version conflicts.
     * With default {@code abort_on_version_conflict}, conflicts are counted and also appear in
     * {@link BulkByScrollResponse#getBulkFailures() bulk failures}; both must be empty for the assertion to pass.
     *
     * @param sliceLabel identifies the reindex step in assertion messages
     * @param response     response from {@code ReindexAction} for a single manual slice (or full reindex)
     */
    private static void assertNoReindexConflicts(String sliceLabel, BulkByScrollResponse response) {
        assertEquals(sliceLabel + " should have no version conflicts", 0, response.getVersionConflicts());
        assertThat(sliceLabel + " bulk failures", response.getBulkFailures(), empty());
    }

    /**
     * Asserts that the destination index contains exactly {@code docCount} documents with Elasticsearch ids
     * {@code "0"} … {@code (docCount - 1)} as strings, with no duplicates and no gaps.
     *
     * @param label     prefix for assertion messages (e.g. {@code "dest1"})
     * @param destIndex destination index name
     * @param docCount  expected number of documents and exclusive upper bound on id values
     */
    private void assertDestinationHasAllDocIds(String label, String destIndex, int docCount) {
        assertHitCount(prepareSearch(destIndex).setSize(docCount), docCount);
        SearchResponse destSearch = prepareSearch(destIndex).setSize(docCount).get();
        try {
            Set<String> destIds = Arrays.stream(destSearch.getHits().getHits()).map(SearchHit::getId).collect(Collectors.toSet());
            assertEquals(label + " should have exactly " + docCount + " unique doc IDs", docCount, destIds.size());
            for (int i = 0; i < docCount; i++) {
                assertTrue(label + " should contain doc " + i, destIds.contains(String.valueOf(i)));
            }
        } finally {
            destSearch.decRef();
        }
    }
}
