/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.action;

import co.elastic.elasticsearch.stateless.commits.GetVirtualBatchedCompoundCommitChunksPressure;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GetVirtualBatchedCompoundCommitChunkActionTests extends ESTestCase {

    public void testPressureReleasedIfChunkAllocationTriggersCircuitBreakerException() throws IOException {
        final var shardId = new ShardId(new Index(randomIdentifier(), randomUUID()), between(0, 3));
        final var request = new GetVirtualBatchedCompoundCommitChunkRequest(
            shardId,
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomIntBetween(1, 10),
            randomIdentifier()
        );

        final var shard = mock(IndexShard.class);
        when(shard.shardId()).thenReturn(shardId);
        when(shard.getOperationPrimaryTerm()).thenReturn(request.getPrimaryTerm());
        final var indexMetadata = mock(IndexMetadata.class);
        when(indexMetadata.getState()).thenReturn(IndexMetadata.State.OPEN);
        when(indexMetadata.getIndex()).thenReturn(shardId.getIndex());
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).build();
        when(indexMetadata.getSettings()).thenReturn(settings);
        final var indexSettings = new IndexSettings(indexMetadata, settings);
        when(shard.indexSettings()).thenReturn(indexSettings);

        final var engine = mock(IndexEngine.class);
        when(shard.getEngineOrNull()).thenReturn(engine);

        final var vbccChunksPressure = new GetVirtualBatchedCompoundCommitChunksPressure(settings, MeterRegistry.NOOP);

        final var bigArrays = mock(BigArrays.class);
        when(bigArrays.newByteArray(eq((long) request.getLength()), eq(false))).thenThrow(
            new CircuitBreakingException("simulated", CircuitBreaker.Durability.TRANSIENT)
        );

        final var latch = new CountDownLatch(1);
        TransportGetVirtualBatchedCompoundCommitChunkAction.primaryShardOperation(
            request,
            shard,
            bigArrays,
            vbccChunksPressure,
            ActionListener.wrap(response -> fail("should have failed"), e -> {
                assertTrue(e instanceof CircuitBreakingException);
                latch.countDown();
            })
        );
        safeAwait(latch);

        assertThat(vbccChunksPressure.getCurrentChunksBytes(), equalTo(0L));
    }
}
