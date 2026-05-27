/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.Bits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.mapper.SemanticTextField;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

/**
 * Transport action for {@code POST /{index}/_semantic_cleanup}.
 *
 * <p>For each primary shard on the local node, this action:
 * <ol>
 *   <li>Opens a Lucene searcher over the shard's current index.</li>
 *   <li>Iterates over all live documents, reading {@code _source} and {@code _id}.</li>
 *   <li>For any document that contains {@code _inference_fields.&lt;field&gt;._staged} entries
 *       whose {@code last_modified} timestamp is older than the effective TTL threshold,
 *       removes those {@code _staged} sub-objects.</li>
 *   <li>Sends the modified documents back via a bulk update request so they pass through
 *       the normal indexing pipeline.</li>
 * </ol>
 *
 * <p>The effective TTL is taken from the request's {@code max_age} parameter when present,
 * falling back to the per-index {@link IndexSettings#INDEX_SEMANTIC_TEXT_STAGED_TTL} setting.
 * A negative TTL value (the sentinel used to disable automatic cleanup) causes the shard
 * operation to skip all documents and return zero counts.
 */
public class TransportStagedSemanticCleanupAction extends TransportBroadcastByNodeAction<
    StagedSemanticCleanupRequest,
    StagedSemanticCleanupResponse,
    TransportStagedSemanticCleanupAction.ShardResult,
    Void> {

    private static final Logger logger = LogManager.getLogger(TransportStagedSemanticCleanupAction.class);

    /** Per-shard result carrying the counts of cleared and failed staged fields. */
    public static final class ShardResult implements Writeable {

        private final int cleared;
        private final int failed;

        public ShardResult(int cleared, int failed) {
            this.cleared = cleared;
            this.failed = failed;
        }

        public ShardResult(StreamInput in) throws IOException {
            this.cleared = in.readVInt();
            this.failed = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(cleared);
            out.writeVInt(failed);
        }

        public int cleared() {
            return cleared;
        }

        public int failed() {
            return failed;
        }
    }

    private final IndicesService indicesService;
    private final ProjectResolver projectResolver;
    private final Client client;

    @Inject
    public TransportStagedSemanticCleanupAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client
    ) {
        super(
            StagedSemanticCleanupAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            StagedSemanticCleanupRequest::new,
            transportService.getThreadPool().executor(ThreadPool.Names.MANAGEMENT)
        );
        this.indicesService = indicesService;
        this.projectResolver = projectResolver;
        this.client = new OriginSettingClient(client, ML_ORIGIN);
    }

    @Override
    protected ShardResult readShardResult(StreamInput in) throws IOException {
        return new ShardResult(in);
    }

    @Override
    protected ResponseFactory<StagedSemanticCleanupResponse, ShardResult> getResponseFactory(
        StagedSemanticCleanupRequest request,
        ClusterState clusterState
    ) {
        return (totalShards, successfulShards, failedShards, shardResults, shardFailures) -> {
            int totalCleared = 0;
            int totalFailed = 0;
            for (ShardResult result : shardResults) {
                totalCleared += result.cleared();
                totalFailed += result.failed();
            }
            return new StagedSemanticCleanupResponse(totalShards, successfulShards, failedShards, shardFailures, totalCleared, totalFailed);
        };
    }

    @Override
    protected StagedSemanticCleanupRequest readRequestFrom(StreamInput in) throws IOException {
        return new StagedSemanticCleanupRequest(in);
    }

    /**
     * Scans all live documents on the given shard for expired staged semantic_text data and
     * removes the {@code _staged} sub-object from any field whose {@code last_modified}
     * timestamp predates the effective TTL threshold.
     *
     * <p>Documents that require changes are re-indexed via a single {@link BulkRequest} so
     * they pass through the normal mapping/indexing pipeline. A failure on any individual
     * document is counted but does not abort processing of the remaining documents.
     */
    @Override
    protected void shardOperation(
        StagedSemanticCleanupRequest request,
        ShardRouting shardRouting,
        Task task,
        Void nodeContext,
        ActionListener<ShardResult> listener
    ) {
        final IndexService indexService = indicesService.indexServiceSafe(shardRouting.index());
        final IndexSettings indexSettings = indexService.getIndexSettings();
        final IndexShard shard = indexService.getShard(shardRouting.id());

        // Determine the effective TTL in milliseconds.
        final long ttlMillis;
        if (request.maxAge() != null) {
            ttlMillis = request.maxAge().millis();
        } else {
            ttlMillis = indexSettings.getSemanticTextStagedTtlMillis();
        }

        // A non-positive TTL means cleanup is disabled for this index.
        if (ttlMillis <= 0) {
            listener.onResponse(new ShardResult(0, 0));
            return;
        }

        final Instant threshold = Instant.now().minusMillis(ttlMillis);
        final String indexName = shardRouting.getIndexName();
        final String fieldFilter = request.field();

        // Collect (docId, updatedSource) pairs for documents that need _staged removed.
        final List<String> docIdsToUpdate = new ArrayList<>();
        final List<Map<String, Object>> updatedSources = new ArrayList<>();

        try (Engine.Searcher searcher = shard.acquireSearcher(Engine.SEARCH_SOURCE)) {
            StoredFieldLoader loader = StoredFieldLoader.create(true, Set.of());
            for (LeafReaderContext leafCtx : searcher.getIndexReader().getContext().leaves()) {
                final int maxDoc = leafCtx.reader().maxDoc();
                final Bits liveDocs = leafCtx.reader().getLiveDocs();
                final LeafStoredFieldLoader leafLoader = loader.getLoader(leafCtx, null);
                for (int docId = 0; docId < maxDoc; docId++) {
                    if (liveDocs != null && liveDocs.get(docId) == false) {
                        continue;
                    }
                    leafLoader.advanceTo(docId);
                    if (leafLoader.source() == null) {
                        continue;
                    }
                    final String id = leafLoader.id();
                    if (id == null) {
                        continue;
                    }
                    @SuppressWarnings("unchecked")
                    Map<String, Object> source = (Map<String, Object>) XContentHelper.convertToMap(
                        leafLoader.source(),
                        false,
                        XContentType.JSON
                    ).v2();

                    if (removeExpiredStagedEntries(source, fieldFilter, threshold)) {
                        docIdsToUpdate.add(id);
                        updatedSources.add(source);
                    }
                }
            }
        } catch (IOException e) {
            listener.onFailure(e);
            return;
        }

        if (docIdsToUpdate.isEmpty()) {
            listener.onResponse(new ShardResult(0, 0));
            return;
        }

        // Build a bulk request with one update per document that had expired staged entries.
        final BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < docIdsToUpdate.size(); i++) {
            final UpdateRequest updateRequest = new UpdateRequest(indexName, docIdsToUpdate.get(i));
            updateRequest.doc(updatedSources.get(i));
            updateRequest.retryOnConflict(3);
            bulkRequest.add(updateRequest);
        }

        logger.debug(
            "Shard [{}] of index [{}]: issuing bulk update for [{}] documents with expired staged semantic data",
            shardRouting.shardId(),
            indexName,
            docIdsToUpdate.size()
        );

        client.bulk(bulkRequest, ActionListener.wrap(bulkResponse -> {
            int cleared = 0;
            int failed = 0;
            for (var item : bulkResponse.getItems()) {
                if (item.isFailed()) {
                    failed++;
                    logger.warn(
                        "Failed to clear staged semantic data for document [{}] in index [{}]: {}",
                        item.getId(),
                        indexName,
                        item.getFailureMessage()
                    );
                } else {
                    cleared++;
                }
            }
            listener.onResponse(new ShardResult(cleared, failed));
        }, listener::onFailure));
    }

    /**
     * Removes {@code _staged} sub-objects from {@code _inference_fields} entries in the
     * provided document source whose {@code last_modified} timestamp is older than
     * {@code threshold}.
     *
     * @param source      the parsed document source map, modified in place
     * @param fieldFilter optional field name to restrict cleanup to; {@code null} means all fields
     * @param threshold   any staged entry with {@code last_modified} before this instant is removed
     * @return {@code true} if at least one {@code _staged} entry was removed
     */
    @SuppressWarnings("unchecked")
    private static boolean removeExpiredStagedEntries(Map<String, Object> source, String fieldFilter, Instant threshold) {
        final Object rawInferenceFields = source.get(InferenceMetadataFieldsMapper.NAME);
        if (rawInferenceFields instanceof Map<?, ?> == false) {
            return false;
        }
        final Map<String, Object> inferenceFields = (Map<String, Object>) rawInferenceFields;
        boolean modified = false;

        for (Map.Entry<String, Object> fieldEntry : inferenceFields.entrySet()) {
            final String fieldName = fieldEntry.getKey();
            if (fieldFilter != null && fieldFilter.equals(fieldName) == false) {
                continue;
            }
            if (fieldEntry.getValue() instanceof Map<?, ?> == false) {
                continue;
            }
            final Map<String, Object> fieldMeta = (Map<String, Object>) fieldEntry.getValue();
            final Object staged = fieldMeta.get(SemanticTextField.STAGED_FIELD);
            if (staged instanceof Map<?, ?> == false) {
                continue;
            }
            final Map<String, Object> stagedMap = (Map<String, Object>) staged;
            final Object lastModifiedObj = stagedMap.get("last_modified");
            if (lastModifiedObj instanceof String == false) {
                continue;
            }
            final String lastModifiedStr = (String) lastModifiedObj;
            final Instant lastModified;
            try {
                lastModified = Instant.parse(lastModifiedStr);
            } catch (Exception e) {
                logger.warn(
                    () -> org.elasticsearch.common.Strings.format(
                        "Skipping staged entry for field [%s]: could not parse last_modified [%s]",
                        fieldName,
                        lastModifiedStr
                    )
                );
                continue;
            }
            if (lastModified.isBefore(threshold)) {
                fieldMeta.remove(SemanticTextField.STAGED_FIELD);
                modified = true;
                logger.debug("Removing expired staged semantic data for field [{}] (last_modified=[{}])", fieldName, lastModified);
            }
        }
        return modified;
    }

    @Override
    protected ShardsIterator shards(ClusterState clusterState, StagedSemanticCleanupRequest request, String[] concreteIndices) {
        return clusterState.routingTable(projectResolver.getProjectId()).allShards(concreteIndices);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, StagedSemanticCleanupRequest request) {
        return state.blocks().globalBlockedException(projectResolver.getProjectId(), ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, StagedSemanticCleanupRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(projectResolver.getProjectId(), ClusterBlockLevel.METADATA_WRITE, concreteIndices);
    }
}
