/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.MappedActionFilter;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

/// Verifies that reindex's PIT pagination only requests an accurate `track_total_hits` on the first batch and
/// disables tracking on every follow-up batch
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class ReindexTrackTotalHitsIT extends ESIntegTestCase {

    private static final String SOURCE_INDEX = "source";
    private static final String DEST_INDEX = "dest";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ReindexPlugin.class, TrackTotalHitsCapturePlugin.class);
    }

    @Before
    public void resetCapture() {
        TrackTotalHitsCapturePlugin.CAPTURED.clear();
    }

    public void testFirstBatchTracksAccuratelyAndSubsequentBatchesDoNot() {
        // Exceed the default cap so disabling tracking on follow-ups actually changes the response total.
        int numDocs = SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO + randomIntBetween(1, SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO);
        createIndex(SOURCE_INDEX);
        int batchSize = 1000;
        indexRandom(true, SOURCE_INDEX, numDocs);
        assertHitCount(prepareSearch(SOURCE_INDEX).setTrackTotalHits(true).setSize(0), numDocs);

        ReindexRequestBuilder reindexRequest = new ReindexRequestBuilder(client()).source(SOURCE_INDEX)
            .destination(DEST_INDEX)
            .refresh(true);
        reindexRequest.source().setSize(batchSize);
        BulkByScrollResponse response = reindexRequest.get();

        assertEquals(numDocs, response.getStatus().getTotal());
        assertEquals(numDocs, response.getCreated());
        assertHitCount(prepareSearch(DEST_INDEX).setTrackTotalHits(true).setSize(0), numDocs);

        // Guard against a single-batch run that would make the assertions below trivially true.
        int expectedMinBatches = (numDocs + batchSize - 1) / batchSize;
        assertThat("reindex must run multiple PIT batches", response.getStatus().getBatches(), greaterThanOrEqualTo(expectedMinBatches));

        // PIT pagination issues one search per batch plus one final empty search that ends the run.
        List<Integer> values = TrackTotalHitsCapturePlugin.CAPTURED;
        assertThat("captured == batches + final empty search", values, hasSize(response.getStatus().getBatches() + 1));
        assertEquals("first batch tracks accurately", Integer.valueOf(SearchContext.TRACK_TOTAL_HITS_ACCURATE), values.getFirst());
        assertThat(
            "every follow-up batch disables tracking",
            values.subList(1, values.size()),
            everyItem(equalTo(SearchContext.TRACK_TOTAL_HITS_DISABLED))
        );
    }

    public void testSlicedReindexTracksTotalHitsPerSlice() {
        int shards = randomIntBetween(2, 4);
        int slices = randomIntBetween(2, 5);
        assertAcked(prepareCreate(SOURCE_INDEX).setSettings(indexSettings(shards, 0)));
        int numDocs = SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO + randomIntBetween(1, SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO);
        indexRandom(true, SOURCE_INDEX, numDocs);
        assertHitCount(prepareSearch(SOURCE_INDEX).setTrackTotalHits(true).setSize(0), numDocs);

        int batchSize = 1000;
        ReindexRequestBuilder reindexRequest = new ReindexRequestBuilder(client()).source(SOURCE_INDEX)
            .destination(DEST_INDEX)
            .refresh(true)
            .setSlices(slices);
        reindexRequest.source().setSize(batchSize);
        BulkByScrollResponse response = reindexRequest.get();
        assertEquals(numDocs, response.getStatus().getTotal());
        assertEquals(numDocs, response.getCreated());
        assertHitCount(prepareSearch(DEST_INDEX).setTrackTotalHits(true).setSize(0), numDocs);

        // Doc-to-slice distribution is hash-based, so assert on totals across slices rather than per-slice batch counts.
        // Total non-empty batches across all slices is at least ceil(numDocs / batchSize).
        assertThat("expected slicing to be applied", response.getStatus().getSliceStatuses(), hasSize(slices));
        int expectedMinBatches = (numDocs + batchSize - 1) / batchSize;
        assertThat(
            "total batches across slices must paginate beyond one per slice",
            response.getStatus().getBatches(),
            greaterThanOrEqualTo(expectedMinBatches)
        );

        // Each slice issues exactly one accurate-tracking first batch; everything else must disable tracking.
        long accurateCount = TrackTotalHitsCapturePlugin.CAPTURED.stream()
            .filter(v -> v == SearchContext.TRACK_TOTAL_HITS_ACCURATE)
            .count();
        long disabledCount = TrackTotalHitsCapturePlugin.CAPTURED.stream()
            .filter(v -> v == SearchContext.TRACK_TOTAL_HITS_DISABLED)
            .count();
        assertEquals("one accurate first batch per slice", slices, (int) accurateCount);
        assertThat("at least one follow-up disables tracking", disabledCount, greaterThan(0L));
        assertEquals(
            "captured requests are only accurate or disabled",
            TrackTotalHitsCapturePlugin.CAPTURED.size(),
            (int) (accurateCount + disabledCount)
        );
    }

    public void testStandaloneSearchDoesNotMutateTrackTotalHits() {
        // Guard: the capture filter only matches PIT searches, not plain searches.
        indexRandom(true, SOURCE_INDEX, 50);
        prepareSearch(SOURCE_INDEX).setTrackTotalHits(true).setSize(10).get().decRef();
        assertThat(TrackTotalHitsCapturePlugin.CAPTURED, hasSize(0));
    }

    /// Captures the `track_total_hits` value of every PIT search that flows through `TransportSearchAction`.
    public static final class TrackTotalHitsCapturePlugin extends Plugin implements ActionPlugin {

        static final List<Integer> CAPTURED = new CopyOnWriteArrayList<>();

        @Override
        public Collection<MappedActionFilter> getMappedActionFilters() {
            return List.of(new MappedActionFilter() {
                @Override
                public String actionName() {
                    return TransportSearchAction.NAME;
                }

                @Override
                public <Request extends ActionRequest, Response extends ActionResponse> void apply(
                    Task task,
                    String action,
                    Request request,
                    ActionListener<Response> listener,
                    ActionFilterChain<Request, Response> chain
                ) {
                    if (request instanceof SearchRequest sr && isPitSearch(sr)) {
                        Integer trackTotalHitsUpTo = sr.source().trackTotalHitsUpTo();
                        assertNotNull(trackTotalHitsUpTo);
                        CAPTURED.add(trackTotalHitsUpTo);
                    }
                    chain.proceed(task, action, request, listener);
                }
            });
        }

        private static boolean isPitSearch(SearchRequest searchRequest) {
            return searchRequest.source() != null && searchRequest.source().pointInTimeBuilder() != null;
        }
    }
}
