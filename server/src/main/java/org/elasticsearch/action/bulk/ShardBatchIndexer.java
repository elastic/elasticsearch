/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineBatch;
import org.elasticsearch.index.mapper.ShardBatchMapper;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.sourcebatch.SourceBatch;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.settings.Setting.boolSetting;

/**
 * Handles the batch indexing code path for primary and replica shards, using the columnar
 * metadata-mapper pipeline ({@link ShardBatchMapper}) rather than per-document row parsing.
 */
public final class ShardBatchIndexer {

    private static final Logger logger = LogManager.getLogger(ShardBatchIndexer.class);

    public static final FeatureFlag BATCH_INDEXING_FEATURE_FLAG = new FeatureFlag("batch_indexing");
    public static final Setting<Boolean> BATCH_INDEXING = boolSetting("indices.batch_indexing", false, value -> {
        if (value && BATCH_INDEXING_FEATURE_FLAG.isEnabled() == false) {
            throw new IllegalArgumentException(
                "[indices.batch_indexing] can only be enabled when the batch_indexing feature flag is enabled"
            );
        }
    }, Setting.Property.NodeScope);

    // Maximum number of operations to parse and index in a single pass to bound memory usage.
    static final int BATCH_CHUNK_SIZE = 5000;

    private final boolean batchIndexingEnabled;
    private final Recycler<BytesRef> recycler;

    ShardBatchIndexer(Settings settings, Recycler<BytesRef> recycler) {
        this.batchIndexingEnabled = BATCH_INDEXING.get(settings);
        this.recycler = recycler;
    }

    public static boolean isBatchIndexingSupported(ClusterService clusterService) {
        return BATCH_INDEXING.get(clusterService.getSettings())
            && BATCH_INDEXING_FEATURE_FLAG.isEnabled()
            && clusterService.state().getMinTransportVersion().supports(BulkShardRequest.BULK_SHARD_BATCH);
    }

    /**
     * Checks whether the batch indexing path can be used for this request.
     * Returns true if batch indexing is enabled, a source batch is present, synthetic source is active,
     * and all operations are index/create (no deletes, no updates).
     */
    public boolean canUseBatchIndexing(BulkShardRequest request) {
        if (batchIndexingEnabled == false) {
            return false;
        }
        if (request.getBulkShardBatch() == null) {
            return false;
        }
        for (BulkItemRequest item : request.items()) {
            final DocWriteRequest.OpType opType = item.request().opType();
            if (opType != DocWriteRequest.OpType.INDEX && opType != DocWriteRequest.OpType.CREATE) {
                return false;
            }
        }
        return true;
    }

    /**
     * Attempts batch indexing on primary using the columnar mapper pipeline.
     */
    void performBatchIndexOnPrimary(
        final BulkItemRequest[] items,
        final SourceBatch batch,
        final BulkPrimaryExecutionContext context,
        final ActionListener<Void> listener
    ) {
        ActionListener.run(listener, l -> {
            doBatchIndexOnPrimary(items, batch, context.getPrimary(), context);
            l.onResponse(null);
        });
    }

    private void doBatchIndexOnPrimary(
        final BulkItemRequest[] items,
        final SourceBatch batch,
        final IndexShard primary,
        final BulkPrimaryExecutionContext context
    ) throws IOException {

        // Check for aborted items upfront
        for (BulkItemRequest item : items) {
            if (item.getPrimaryResponse() != null
                && item.getPrimaryResponse().isFailed()
                && item.getPrimaryResponse().getFailure().isAborted()) {
                return;
            }
        }

        // Resolve every schema column to a mapper once per batch. If any column is outside the
        // batch-indexing support matrix this returns null, and we fall back to the sequential
        // path (same contract as a later parseMappings returning null).
        final ShardBatchMapper.BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(
            batch.schema(),
            primary.mapperService().mappingLookup(),
            primary.indexSettings()
        );
        if (resolution == null) {
            return;
        }

        for (int chunkStart = 0; chunkStart < items.length; chunkStart += BATCH_CHUNK_SIZE) {
            final int chunkEnd = Math.min(chunkStart + BATCH_CHUNK_SIZE, items.length);
            final EngineBatch engineBatch = ShardBatchMapper.mapColumnBatch(
                items,
                batch,
                primary,
                chunkStart,
                chunkEnd,
                resolution,
                Engine.Operation.Origin.PRIMARY,
                recycler
            );
            if (engineBatch == null) {
                return;
            }

            final List<Engine.IndexResult> results = primary.applyIndexOperationBatchOnPrimary(engineBatch);
            logger.trace("batch indexed [{}] operations on primary shard [{}]", results.size(), primary.shardId());

            for (Engine.IndexResult result : results) {
                assert context.hasMoreOperationsToExecute();
                context.setRequestToExecute(context.getCurrent());
                context.markBatchOperationAsExecuted(result);
                context.markAsCompleted(context.getExecutionResult());
            }

        }
    }

    /**
     * Attempts batch indexing on replica using the columnar metadata-mapper pipeline.
     *
     * <p>Within each chunk, a failed or NOOP primary response also ends the contiguous valid run; those
     * items and any remainder fall back to sequential processing via the returned {@code processedItems}.
     */
    ReplicaBatchResult performBatchIndexOnReplica(BulkItemRequest[] items, SourceBatch batch, IndexShard replica) throws Exception {
        final ShardBatchMapper.BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(
            batch.schema(),
            replica.mapperService().mappingLookup(),
            replica.indexSettings()
        );
        if (resolution == null) {
            return new ReplicaBatchResult(0, null);
        }

        Translog.Location location = null;
        int processedItems = 0;

        for (int chunkStart = 0; chunkStart < items.length; chunkStart += BATCH_CHUNK_SIZE) {
            final int chunkEnd = Math.min(chunkStart + BATCH_CHUNK_SIZE, items.length);

            // Find the end of the contiguous valid run within this chunk. A failed or NOOP primary
            // response ends the run; the remainder falls back to sequential processing.
            // A batch is written as a single contiguous IndexOperationBatch.TranslogRecord, so a primary
            // no-op in the middle of a chunk ends the batch here (rather than being skipped).
            // TODO: This will be resolved in a follow-up to allow the engine level batch execution
            // to handle mixed index and no-op operations.
            int validEnd = chunkStart;
            while (validEnd < chunkEnd) {
                final BulkItemResponse response = items[validEnd].getPrimaryResponse();
                if (response.isFailed()) {
                    break;
                }
                if (response.getResponse().getResult() == DocWriteResponse.Result.NOOP) {
                    break;
                }
                validEnd++;
            }

            if (validEnd > chunkStart) {
                final EngineBatch engineBatch = ShardBatchMapper.mapColumnBatch(
                    items,
                    batch,
                    replica,
                    chunkStart,
                    validEnd,
                    resolution,
                    Engine.Operation.Origin.REPLICA,
                    recycler
                );
                if (engineBatch == null) {
                    processedItems = chunkStart;
                    break;
                }
                final List<Engine.IndexResult> results = replica.applyIndexOperationBatchOnReplica(engineBatch);
                for (Engine.IndexResult result : results) {
                    if (result.getFailure() != null) {
                        throw result.getFailure();
                    }
                    location = TransportWriteAction.locationToSync(location, result.getTranslogLocation(), true);
                }
            }

            if (validEnd < chunkEnd) {
                processedItems = validEnd;
                break;
            }

            processedItems = chunkEnd;
        }

        return new ReplicaBatchResult(processedItems, location);
    }

    record ReplicaBatchResult(int processedItems, @Nullable Translog.Location location) {}
}
