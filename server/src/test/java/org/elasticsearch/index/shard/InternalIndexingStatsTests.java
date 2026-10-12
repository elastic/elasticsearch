/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.engine.IndexOperationBatch;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InternalIndexingStatsTests extends ESTestCase {

    public void testPollUtilizationTracksSuccessfulIndexingTime() {
        AtomicLong currentTime = new AtomicLong(0);
        final int numThreads = randomIntBetween(1, 8);
        InternalIndexingStats internalIndexingStats = new InternalIndexingStats(
            () -> currentTime.get(),
            new IndexingStatsSettings(ClusterSettings.createBuiltInClusterSettings()),
            numThreads
        );

        ParsedDocument doc = EngineTestCase.createParsedDoc("1", null);
        Engine.Index index = new Engine.Index(Uid.encodeId(doc.id()), 1L, doc);
        Engine.IndexResult result = mock(Engine.IndexResult.class);
        when(result.getResultType()).thenReturn(Engine.Result.Type.SUCCESS);
        final long operationTimeNanos = 500_000;
        when(result.getTook()).thenReturn(operationTimeNanos);

        // Verify that utilization is zero before any writes are done.
        currentTime.set(1000L);
        assertThat(internalIndexingStats.pollUtilization(), equalTo(0.0d));

        internalIndexingStats.preIndex(null /* unused */, index);
        internalIndexingStats.postIndex(null /* unused */, index, result);
        currentTime.set(operationTimeNanos + 1000L);  // Advance time by the operation time to simulate the passage of time.
        final double utilization = internalIndexingStats.pollUtilization();

        // Verify that utilization is no longer zero now that a write operation has occurred.
        assertThat(utilization, greaterThan(0.0d));

        // One operation ran the duration of the time window polled. So the utilization is relative to the number of threads available.
        assertEquals(1.0 / numThreads, utilization, 0.0001);
    }

    /**
     * The batch hooks must record the same stats as the equivalent sequence of per-operation hooks,
     * for a mix of successes, document failures and version conflicts.
     */
    public void testBatchHooksMatchPerOperationHooks() {
        AtomicLong currentTime = new AtomicLong(randomLongBetween(0, 1_000_000_000L));
        InternalIndexingStats perOpStats = newStats(currentTime);
        InternalIndexingStats batchStats = newStats(currentTime);
        ShardId shardId = new ShardId(new Index("index", "_na_"), 0);

        final int docCount = randomIntBetween(3, 16);
        final IndexOperationBatch batch = primaryBatch(docCount);
        final List<Engine.IndexResult> results = new ArrayList<>(docCount);
        // guarantee at least one success, one document failure and one version conflict, then randomize the rest
        results.add(successResult(randomLongBetween(1, 1_000_000)));
        results.add(failureResult(batch.id(1), new RuntimeException("doc failure")));
        results.add(failureResult(batch.id(2), new VersionConflictEngineException(shardId, "2", "conflict")));
        for (int d = 3; d < docCount; d++) {
            results.add(randomResult(shardId, batch.id(d)));
        }

        final List<Engine.Index> ops = batch.materializeIndexOps();
        for (Engine.Index op : ops) {
            perOpStats.preIndex(shardId, op);
        }
        for (int d = 0; d < docCount; d++) {
            perOpStats.postIndex(shardId, ops.get(d), results.get(d));
        }

        batchStats.preIndexBatch(shardId, batch);
        batchStats.postIndexBatch(shardId, batch, results);

        assertStatsEqual(perOpStats, batchStats, currentTime.get());
    }

    public void testBatchEngineFailureHookMatchesPerOperationHooks() {
        AtomicLong currentTime = new AtomicLong(randomLongBetween(0, 1_000_000_000L));
        InternalIndexingStats perOpStats = newStats(currentTime);
        InternalIndexingStats batchStats = newStats(currentTime);
        ShardId shardId = new ShardId(new Index("index", "_na_"), 0);

        final int docCount = randomIntBetween(1, 16);
        final IndexOperationBatch batch = primaryBatch(docCount);
        final Exception failure = new RuntimeException("engine failure");

        final List<Engine.Index> ops = batch.materializeIndexOps();
        for (Engine.Index op : ops) {
            perOpStats.preIndex(shardId, op);
        }
        for (Engine.Index op : ops) {
            perOpStats.postIndex(shardId, op, failure);
        }

        batchStats.preIndexBatch(shardId, batch);
        batchStats.postIndexBatch(shardId, batch, failure);

        assertStatsEqual(perOpStats, batchStats, currentTime.get());
    }

    private static InternalIndexingStats newStats(AtomicLong currentTime) {
        return new InternalIndexingStats(
            currentTime::get,
            new IndexingStatsSettings(ClusterSettings.createBuiltInClusterSettings()),
            randomIntBetween(1, 8)
        );
    }

    private static IndexOperationBatch primaryBatch(int docCount) {
        final BulkItemRequest[] items = new BulkItemRequest[docCount];
        for (int d = 0; d < docCount; d++) {
            items[d] = new BulkItemRequest(
                d,
                new IndexRequest("index").id("doc-" + d).source(new BytesArray("{\"n\":" + d + "}"), XContentType.JSON)
            );
        }
        return IndexOperationBatch.initFromBulk(items, 0, docCount, null, Engine.Operation.Origin.PRIMARY, 1L, 0L);
    }

    private static Engine.IndexResult randomResult(ShardId shardId, String id) {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> successResult(randomLongBetween(1, 1_000_000));
            case 1 -> failureResult(id, new RuntimeException("doc failure"));
            case 2 -> failureResult(id, new VersionConflictEngineException(shardId, id, "conflict"));
            default -> throw new AssertionError("unexpected");
        };
    }

    /** Mocked because {@code Engine.Result#setTook}/{@code freeze} are package-private to the engine package. */
    private static Engine.IndexResult successResult(long tookNanos) {
        Engine.IndexResult result = mock(Engine.IndexResult.class);
        when(result.getResultType()).thenReturn(Engine.Result.Type.SUCCESS);
        when(result.getTook()).thenReturn(tookNanos);
        return result;
    }

    private static Engine.IndexResult failureResult(String id, Exception failure) {
        return new Engine.IndexResult(failure, 1, 1, SequenceNumbers.UNASSIGNED_SEQ_NO, id);
    }

    private static void assertStatsEqual(InternalIndexingStats expected, InternalIndexingStats actual, long timeInNanos) {
        IndexingStats.Stats expectedStats = statsOf(expected, timeInNanos);
        IndexingStats.Stats actualStats = statsOf(actual, timeInNanos);
        assertThat(actualStats.getIndexCount(), equalTo(expectedStats.getIndexCount()));
        assertThat(actualStats.getIndexTime(), equalTo(expectedStats.getIndexTime()));
        assertThat(expectedStats.getIndexCurrent(), equalTo(0L));
        assertThat(actualStats.getIndexCurrent(), equalTo(0L));
        assertThat(actualStats.getIndexFailedCount(), equalTo(expectedStats.getIndexFailedCount()));
        assertThat(actualStats.getIndexFailedDueToVersionConflictCount(), equalTo(expectedStats.getIndexFailedDueToVersionConflictCount()));
        assertEquals(expected.totalIndexingTimeInNanos(), actual.totalIndexingTimeInNanos());
        assertEquals(expected.totalIndexingExecutionTimeInNanos(), actual.totalIndexingExecutionTimeInNanos());
        // one summed EWMA increment vs N increments at the same instant: equal up to floating point rounding
        double expectedLoad = expected.recentIndexingLoad(timeInNanos);
        assertEquals(expectedLoad, actual.recentIndexingLoad(timeInNanos), Math.abs(expectedLoad) * 1e-6 + 1e-12);
    }

    private static IndexingStats.Stats statsOf(InternalIndexingStats stats, long timeInNanos) {
        return stats.stats(false, 0, 0, 0, 1, timeInNanos, 0).getTotal();
    }
}
