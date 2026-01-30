/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.index.reindex.AbstractBulkByScrollRequest.DEFAULT_SCROLL_TIMEOUT;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

@ESIntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0, scope = ESIntegTestCase.Scope.TEST)
public class ReindexResumeIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ReindexPlugin.class);
    }

    public void testResumeReindexFromScroll_noSlicing() {
        String sourceIndex = "source-index";
        String destIndex = "dest-index";
        int totalDocs = randomIntBetween(20, 100);
        int batchSize = randomIntBetween(1, 10);

        createIndex(sourceIndex);
        indexRandom(true, sourceIndex, totalDocs);

        // Manually initiate a scroll search to get a scroll ID
        String scrollId;
        int remainingDocs = totalDocs - batchSize;
        SearchResponse searchResponse = client().prepareSearch(sourceIndex).setScroll(DEFAULT_SCROLL_TIMEOUT).setSize(batchSize).get();
        try {
            scrollId = searchResponse.getScrollId();
            assertNotNull(scrollId);
            assertEquals((int) searchResponse.getHits().getTotalHits().value(), totalDocs);
            assertEquals(searchResponse.getHits().getHits().length, batchSize);
        } finally {
            searchResponse.decRef();
        }

        // Resume reindexing from the manual scroll search
        BulkByScrollTask.Status randomStats = randomStats();
        ReindexRequest request = new ReindexRequest().setSourceIndices(sourceIndex)
            .setDestIndex(destIndex)
            .setSourceBatchSize(batchSize)
            .setRefresh(true)
            .setResumeInfo(
                new AbstractBulkByScrollRequest.ResumeInfo(
                    new AbstractBulkByScrollRequest.ScrollWorkerResumeInfo(scrollId, randomStats),
                    null
                )
            );
        BulkByScrollResponse response = client().execute(ReindexAction.INSTANCE, request).actionGet();

        // total should equal to total hits from the search
        assertEquals(totalDocs, response.getTotal());
        // stats are updated
        assertEquals(remainingDocs + randomStats.getCreated(), response.getCreated());
        int remainingBatches = remainingDocs / batchSize + (remainingDocs % batchSize == 0 ? 0 : 1);
        assertEquals(remainingBatches + randomStats.getBatches(), response.getBatches());
        // other stats should be retained
        assertEquals(randomStats.getDeleted(), response.getDeleted());
        assertEquals(randomStats.getUpdated(), response.getUpdated());
        assertEquals(randomStats.getVersionConflicts(), response.getVersionConflicts());
        assertEquals(randomStats.getNoops(), response.getNoops());
        assertEquals(randomStats.getBulkRetries(), response.getBulkRetries());
        assertEquals(randomStats.getSearchRetries(), response.getSearchRetries());
        assertEquals(randomStats.getRequestsPerSecond(), response.getStatus().getRequestsPerSecond(), 0);

        // ensure remaining docs were indexed
        assertHitCount(prepareSearch(destIndex), remainingDocs);
    }

    private BulkByScrollTask.Status randomStats() {
        return new BulkByScrollTask.Status(
            null,
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeInt(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomTimeValue(),
            randomNonNegativeLong(),
            null,
            TimeValue.ZERO
        );
    }
}
