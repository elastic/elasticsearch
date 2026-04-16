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
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.plugins.internal.XContentMeteringParserDecorator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.common.settings.Setting.boolSetting;

/**
 * Handles the batch indexing code path for primary and replica shards.
 */
public final class ShardBatchIndexer {

    public static final FeatureFlag BATCH_INDEXING_FEATURE_FLAG = new FeatureFlag("batch_indexing");
    public static final Setting<Boolean> BATCH_INDEXING = boolSetting("indices.batch_indexing", false, value -> {
        if (value && BATCH_INDEXING_FEATURE_FLAG.isEnabled() == false) {
            throw new IllegalArgumentException(
                "[indices.batch_indexing] can only be enabled when the batch_indexing feature flag is enabled"
            );
        }
    }, Setting.Property.NodeScope);

    // Maximum number of operations to parse and index in a single pass to bound memory usage.
    static final int BATCH_CHUNK_SIZE = 32;

    private ShardBatchIndexer() {}

    /**
     * Checks whether the batch indexing path can be used for this request.
     * Returns true if batch indexing is enabled and all operations are index/create (no deletes, no updates).
     */
    public static boolean canUseBatchIndexing(BulkShardRequest request, boolean batchIndexingEnabled) {
        if (batchIndexingEnabled == false) {
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
     * Attempts batch indexing on primary. The IndexShard is obtained from {@code context.getPrimary()}.
     * On success, the caller checks {@link BulkPrimaryExecutionContext#hasMoreOperationsToExecute()} to
     * determine whether the sequential fallback path is needed for the remaining items.
     */
    static void performBatchIndexOnPrimary(
        final BulkShardRequest request,
        final DocumentParsingProvider documentParsingProvider,
        final BulkPrimaryExecutionContext context,
        final ActionListener<Void> listener
    ) {
        ActionListener.run(listener, l -> {
            doBatchIndexOnPrimary(request, context.getPrimary(), documentParsingProvider, context);
            l.onResponse(null);
        });
    }

    private static void doBatchIndexOnPrimary(
        final BulkShardRequest request,
        final IndexShard primary,
        final DocumentParsingProvider documentParsingProvider,
        final BulkPrimaryExecutionContext context
    ) throws IOException {
        final BulkItemRequest[] items = request.items();

        // Check for aborted items upfront
        for (BulkItemRequest item : items) {
            if (item.getPrimaryResponse() != null
                && item.getPrimaryResponse().isFailed()
                && item.getPrimaryResponse().getFailure().isAborted()) {
                return;
            }
        }

        // TODO: Required because VerionLock is re-entrant. We likely can switch that to be semaphore based and remove this protection
        final Set<BytesRef> seenUids = new HashSet<>(Math.min(items.length, BATCH_CHUNK_SIZE));

        // Process in chunks to bound memory: parse + index BATCH_CHUNK_SIZE docs at a time,
        // allowing previous chunks' parsed docs to be GC'd before parsing the next chunk.
        for (int chunkStart = 0; chunkStart < items.length; chunkStart += BATCH_CHUNK_SIZE) {
            final int chunkEnd = Math.min(chunkStart + BATCH_CHUNK_SIZE, items.length);
            final int chunkSize = chunkEnd - chunkStart;
            final List<Engine.Index> operations = new ArrayList<>(chunkSize);

            for (int i = chunkStart; i < chunkEnd; i++) {
                final IndexRequest indexRequest = (IndexRequest) items[i].request();
                final XContentMeteringParserDecorator meteringParserDecorator = documentParsingProvider.newMeteringParserDecorator(
                    indexRequest
                );
                final SourceToParse sourceToParse = new SourceToParse(
                    indexRequest.id(),
                    indexRequest.source(),
                    indexRequest.getContentType(),
                    indexRequest.routing(),
                    indexRequest.getDynamicTemplates(),
                    indexRequest.getDynamicTemplateParams(),
                    indexRequest.getIncludeSourceOnError(),
                    meteringParserDecorator,
                    indexRequest.tsid()
                );
                Engine.Index operation;
                try {
                    operation = IndexShard.prepareIndex(
                        primary.mapperService(),
                        sourceToParse,
                        SequenceNumbers.UNASSIGNED_SEQ_NO,
                        primary.getOperationPrimaryTerm(),
                        indexRequest.version(),
                        indexRequest.versionType(),
                        Engine.Operation.Origin.PRIMARY,
                        indexRequest.getAutoGeneratedTimestamp(),
                        indexRequest.isRetry(),
                        indexRequest.ifSeqNo(),
                        indexRequest.ifPrimaryTerm(),
                        primary.getRelativeTimeInNanos()
                    );
                } catch (Exception e) {
                    return;
                }
                if (operation.parsedDoc().dynamicMappingsUpdate() != null) {
                    return;
                }
                if (seenUids.add(operation.uid()) == false) {
                    return;
                }
                operations.add(operation);
            }

            final List<Engine.IndexResult> results = primary.applyIndexOperationBatchOnPrimary(operations);

            for (Engine.IndexResult result : results) {
                assert context.hasMoreOperationsToExecute();
                context.setRequestToExecute(context.getCurrent());
                context.markOperationAsExecuted(result);
                context.markAsCompleted(context.getExecutionResult());
            }
            seenUids.clear();
        }
    }

    /**
     * Performs a batch index on a replica. Returns the number of items processed from the start of the request's
     * items array. The caller should fall back to the item-by-item path for any remaining items.
     * The returned location may be null if no operations produced a translog location.
     */
    static ReplicaBatchResult performBatchIndexOnReplica(BulkShardRequest request, IndexShard replica) throws Exception {
        final BulkItemRequest[] items = request.items();
        // TODO: Required because VerionLock is re-entrant. We likely can switch that to be semaphore based and remove this protection
        final Set<BytesRef> seenUids = new HashSet<>(Math.min(items.length, BATCH_CHUNK_SIZE));
        Translog.Location location = null;
        int processedItems = 0;

        for (int chunkStart = 0; chunkStart < items.length; chunkStart += BATCH_CHUNK_SIZE) {
            final int chunkEnd = Math.min(chunkStart + BATCH_CHUNK_SIZE, items.length);
            final List<Engine.Index> operations = new ArrayList<>(chunkEnd - chunkStart);

            int i = chunkStart;
            while (i < chunkEnd) {
                final BulkItemRequest item = items[i];
                final BulkItemResponse response = item.getPrimaryResponse();

                if (response.isFailed()) {
                    break;
                }
                if (response.getResponse().getResult() == DocWriteResponse.Result.NOOP) {
                    i++;
                    continue;
                }
                assert response.getResponse().getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO;

                final IndexRequest indexRequest = (IndexRequest) item.request();
                final DocWriteResponse primaryResponse = response.getResponse();
                final SourceToParse sourceToParse = TransportShardBulkAction.replicaSourceToParse(indexRequest);
                Engine.Index operation;
                try {
                    operation = IndexShard.prepareIndex(
                        replica.mapperService(),
                        sourceToParse,
                        primaryResponse.getSeqNo(),
                        primaryResponse.getPrimaryTerm(),
                        primaryResponse.getVersion(),
                        null,
                        Engine.Operation.Origin.REPLICA,
                        indexRequest.getAutoGeneratedTimestamp(),
                        indexRequest.isRetry(),
                        SequenceNumbers.UNASSIGNED_SEQ_NO,
                        0,
                        replica.getRelativeTimeInNanos()
                    );
                } catch (Exception e) {
                    break;
                }
                if (operation.parsedDoc().dynamicMappingsUpdate() != null) {
                    break;
                }
                if (seenUids.add(operation.uid()) == false) {
                    break;
                }
                operations.add(operation);
                i++;
            }

            if (operations.isEmpty() == false) {
                final List<Engine.IndexResult> results = replica.applyIndexOperationBatchOnReplica(operations);
                for (Engine.IndexResult result : results) {
                    if (result.getFailure() != null) {
                        throw result.getFailure();
                    }
                    location = TransportWriteAction.locationToSync(location, result.getTranslogLocation());
                }
            }

            if (i < chunkEnd) {
                processedItems = i;
                break;
            }

            processedItems = chunkEnd;
            seenUids.clear();
        }

        return new ReplicaBatchResult(processedItems, location);
    }

    record ReplicaBatchResult(int processedItems, @Nullable Translog.Location location) {}
}
