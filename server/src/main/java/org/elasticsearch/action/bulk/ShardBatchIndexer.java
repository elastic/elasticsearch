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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.eirf.EirfBatch;
import org.elasticsearch.eirf.EirfRowReader;
import org.elasticsearch.eirf.EirfRowToXContent;
import org.elasticsearch.eirf.EirfRowXContentParser;
import org.elasticsearch.eirf.EirfSchema;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.internal.XContentMeteringParserDecorator;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.settings.Setting.boolSetting;

/**
 * Handles the EIRF batch indexing code path for primary and replica shards.
 * Documents are read directly from an {@link EirfBatch} using {@link EirfRowXContentParser}
 * to feed the document parsing pipeline without intermediate JSON serialization.
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
    static final int BATCH_CHUNK_SIZE = 32;

    private ShardBatchIndexer() {}

    /**
     * Checks whether the batch indexing path can be used for this request.
     * Returns true if batch indexing is enabled, an EIRF batch is present, synthetic source is active,
     * and all operations are index/create (no deletes, no updates).
     */
    public static boolean canUseBatchIndexing(BulkShardRequest request, boolean batchIndexingEnabled) {
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
     * Attempts batch indexing on primary using EIRF data. Each document is parsed from the
     * corresponding row in the batch using an {@link EirfRowXContentParser}.
     */
    static void performBatchIndexOnPrimary(
        final BulkItemRequest[] items,
        final EirfBatch batch,
        final BulkPrimaryExecutionContext context,
        final ActionListener<Void> listener
    ) {
        ActionListener.run(listener, l -> {
            doBatchIndexOnPrimary(items, batch, context.getPrimary(), context);
            l.onResponse(null);
        });
    }

    private static void doBatchIndexOnPrimary(
        final BulkItemRequest[] items,
        final EirfBatch batch,
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

        // TODO: Required because VersionLock is re-entrant. We likely can switch that to be semaphore based and remove this protection
        final Set<BytesRef> seenUids = new HashSet<>(Math.min(items.length, BATCH_CHUNK_SIZE));
        final EirfRowXContentParser.SchemaNode schemaTree = EirfRowXContentParser.buildSchemaTree(batch.schema());

        for (int chunkStart = 0; chunkStart < items.length; chunkStart += BATCH_CHUNK_SIZE) {
            final int chunkEnd = Math.min(chunkStart + BATCH_CHUNK_SIZE, items.length);
            final List<Engine.Index> operations = new ArrayList<>(chunkEnd - chunkStart);

            for (int i = chunkStart; i < chunkEnd; i++) {
                final IndexRequest indexRequest = (IndexRequest) items[i].request();
                final EirfRowReader row = batch.getRowReader(i);
                final EirfRowXContentParser parser = new EirfRowXContentParser(schemaTree, row);

                final XContentType xContentType = indexRequest.getContentType() != null ? indexRequest.getContentType() : XContentType.JSON;
                // TODO: Right now we materialize a source back to avoid breaking translog assertions. We should fix the translog assertions
                // and move to just materializing the original x-content source for stored source mapping
                final BytesReference source = rowToSource(row, batch.schema(), xContentType);
                // TODO: Metering and getIncludeSourceOnError currently do not work with EIRF parsing
                final SourceToParse sourceToParse = new SourceToParse(
                    indexRequest.id(),
                    source,
                    xContentType,
                    indexRequest.routing(),
                    indexRequest.getDynamicTemplates(),
                    indexRequest.getDynamicTemplateParams(),
                    indexRequest.getIncludeSourceOnError(),
                    XContentMeteringParserDecorator.NOOP,
                    indexRequest.tsid(),
                    parser
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
                    logger.warn("batch indexing on primary failed to prepare index for item [{}], falling back", i, e);
                    return;
                }
                if (operation.parsedDoc().dynamicMappingsUpdate() != null) {
                    logger.debug("batch indexing on primary encountered dynamic mapping update at item [{}], falling back", i);
                    return;
                }
                if (seenUids.add(operation.uid()) == false) {
                    logger.debug("batch indexing on primary encountered duplicate uid at item [{}], falling back", i);
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
     * Performs a batch index on a replica using EIRF data.
     */
    static ReplicaBatchResult performBatchIndexOnReplica(BulkItemRequest[] items, EirfBatch batch, IndexShard replica) throws Exception {
        final Set<BytesRef> seenUids = new HashSet<>(Math.min(items.length, BATCH_CHUNK_SIZE));
        final EirfRowXContentParser.SchemaNode schemaTree = EirfRowXContentParser.buildSchemaTree(batch.schema());
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
                final EirfRowReader row = batch.getRowReader(i);
                final EirfRowXContentParser parser = new EirfRowXContentParser(schemaTree, row);

                final XContentType xContentType = indexRequest.getContentType() != null ? indexRequest.getContentType() : XContentType.JSON;
                final BytesReference source = rowToSource(row, batch.schema(), xContentType);
                final SourceToParse sourceToParse = new SourceToParse(
                    indexRequest.id(),
                    source,
                    xContentType,
                    indexRequest.routing(),
                    Map.of(),
                    Map.of(),
                    indexRequest.getIncludeSourceOnError(),
                    XContentMeteringParserDecorator.NOOP,
                    indexRequest.tsid(),
                    parser
                );
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
                    logger.warn("batch indexing on replica failed to prepare index for item [{}], falling back", i, e);
                    break;
                }
                if (operation.parsedDoc().dynamicMappingsUpdate() != null) {
                    logger.debug("batch indexing on replica encountered dynamic mapping update at item [{}], falling back", i);
                    break;
                }
                if (seenUids.add(operation.uid()) == false) {
                    logger.debug("batch indexing on replica encountered duplicate uid at item [{}], falling back", i);
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

    static BytesReference rowToSource(EirfRowReader row, EirfSchema schema, XContentType xContentType) throws IOException {
        try (XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())) {
            EirfRowToXContent.writeRow(row, schema, builder);
            return BytesReference.bytes(builder);
        }
    }

    record ReplicaBatchResult(int processedItems, @Nullable Translog.Location location) {}
}
