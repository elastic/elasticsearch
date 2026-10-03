/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.index.Index;
import org.elasticsearch.sourcebatch.SourceBatch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Pre-routing {@code XContent → ESCF} encode pass. Runs once over the bulk request before
 * {@link BulkOperation#groupRequestsByShards} resolves concrete indices, producing one
 * {@link EscfEncoder}-built {@link org.elasticsearch.escf.EscfBatch} per index abstraction (data
 * stream, concrete index, or plain alias). The resulting batches are handed to
 * {@link BatchRouterSet#forBatches} and then routed and scattered by {@link BatchModeRouter} in
 * the normal way — the same path used by external producers that call
 * {@link BulkRequest#setPreBuiltBatches}.
 *
 * <p>Bulk-wide all-or-nothing: if any item is structurally ineligible
 * ({@link #isBulkBatchEligible}), or if any item's abstraction resolves to a {@code null} batch
 * key (e.g. a direct write to a backing index of a data stream), or if any item's write index
 * uses a routing strategy that requires extracting fields from the source on the shard side
 * (e.g. routing-path-based TSDB), or if any item's source bytes fail encoding, {@link #encode}
 * returns {@code null} and the whole bulk takes the row path.
 * Because attachment ({@link org.elasticsearch.action.index.IndexSource#setSourceRow}) is deferred
 * until every document has been encoded successfully, every {@link IndexRequest} still holds its
 * original inline source on abort, and the row path picks it up without any recovery step.
 */
final class BulkBatchEncoders {

    private static final Logger logger = LogManager.getLogger(BulkBatchEncoders.class);

    private record PendingAttachment(IndexRequest indexRequest, String key, int rowIndex) {}

    private BulkBatchEncoders() {}

    /**
     * Returns true if every item in {@code bulkRequest} is structurally eligible to be batch-encoded:
     * an {@link IndexRequest} with inline source bytes, a known content type, and no pre-attached
     * batch row. If false, the bulk goes through the inline-source path end-to-end and no encoder
     * helper is created.
     */
    static boolean isBulkBatchEligible(BulkRequest bulkRequest) {
        if (bulkRequest.isSimulated()) {
            return false;
        }
        for (DocWriteRequest<?> request : bulkRequest.requests) {
            if (request instanceof IndexRequest indexRequest) {
                if (isItemBatchEligible(indexRequest) == false) {
                    return false;
                }
            } else {
                return false;
            }
        }
        return bulkRequest.requests.isEmpty() == false;
    }

    /**
     * Per-item batch eligibility. Used by {@link #isBulkBatchEligible}; exposed for tests so the
     * pre-scan logic can be exercised in isolation.
     */
    static boolean isItemBatchEligible(IndexRequest request) {
        return request.indexSource().hasSource() && request.getContentType() != null && request.indexSource().hasSourceRow() == false;
    }

    /**
     * Encodes every item in {@code bulkRequest} from x-content into ESCF, groups them by index
     * abstraction, and returns a {@link BatchRouterSet} ready for the routing pass.
     *
     * <p>Returns {@code null} if the bulk is not eligible (see {@link #isBulkBatchEligible}), if any
     * item targets a null batch key, if any write index uses routing that requires source-extraction
     * at the shard (e.g. {@link IndexRouting.ExtractFromSource.ForRoutingPath}), or if encoding
     * fails for any item. On {@code null} return every {@link IndexRequest} still holds its
     * original inline source bytes.
     */
    @Nullable
    static BatchRouterSet encode(BulkRequest bulkRequest, ProjectMetadata project, IndexNameExpressionResolver resolver) {
        if (isBulkBatchEligible(bulkRequest) == false) {
            return null;
        }

        // One encoder per index abstraction key.
        Map<String, EscfEncoder> encoders = new HashMap<>();
        // Per-key abstraction cache: request.index() → batchKey. Resolved once per unique name;
        // a null value (stored explicitly) means "seen and incompatible — abort on first access".
        // We use containsKey() rather than computeIfAbsent() to allow caching null keys.
        Map<String, String> keyByTargetName = new HashMap<>();
        // Deferred list of (request, key, row) to attach after all documents encoded and batches built.
        List<PendingAttachment> pending = new ArrayList<>(bulkRequest.requests.size());
        // Built batches, tracked separately so they can be released on failure after the build loop.
        Map<String, SourceBatch> batches = null;

        try {
            for (DocWriteRequest<?> docRequest : bulkRequest.requests) {
                IndexRequest indexRequest = (IndexRequest) docRequest; // safe: isBulkBatchEligible checked
                String targetName = indexRequest.index();

                // Resolve batch key, memoized per unique target name.
                final String key;
                if (keyByTargetName.containsKey(targetName)) {
                    key = keyByTargetName.get(targetName);
                } else {
                    IndexAbstraction ia = resolver.resolveWriteIndexAbstraction(project, indexRequest);
                    String resolved = BatchModeRouter.batchKey(ia, project);
                    if (resolved != null && isBatchRoutingCompatible(ia, project) == false) {
                        resolved = null; // unsupported routing — treat same as null key
                    }
                    keyByTargetName.put(targetName, resolved); // cache, including null
                    key = resolved;
                }

                if (key == null) {
                    // Direct write to a backing index, alias with no write index, or unsupported routing.
                    logger.debug("batch encoding skipped: item targeting [{}] is not batch-compatible", targetName);
                    return null;
                }

                EscfEncoder encoder = encoders.computeIfAbsent(key, k -> new EscfEncoder());
                int rowIndex = encoder.addDocument(indexRequest.indexSource().bytes(), indexRequest.getContentType());
                // Attachment is deferred: indexSource().bytes() must remain intact for the row path fallback.
                pending.add(new PendingAttachment(indexRequest, key, rowIndex));
            }

            // Build one EscfBatch per abstraction key.
            batches = new HashMap<>(encoders.size() * 2);
            for (Map.Entry<String, EscfEncoder> entry : encoders.entrySet()) {
                batches.put(entry.getKey(), entry.getValue().build());
            }
            // Close encoders now that their batches own the column data.
            for (EscfEncoder encoder : encoders.values()) {
                encoder.close();
            }
            encoders.clear();

            // Attach source-row references to each IndexRequest. Done only after every document
            // encoded and every batch built — so any failure leaves all inline bytes intact.
            for (PendingAttachment attachment : pending) {
                SourceBatch batch = batches.get(attachment.key());
                attachment.indexRequest()
                    .indexSource()
                    .setSourceRow(batch, attachment.rowIndex(), attachment.indexRequest().getContentType());
            }

            return BatchRouterSet.forBatches(batches);

        } catch (Exception e) {
            logger.debug("batch encoding failed; falling back to the row path for this bulk", e);
            // Release any batches that were successfully built.
            if (batches != null) {
                for (SourceBatch batch : batches.values()) {
                    batch.close();
                }
            }
            // Close any open encoders.
            for (EscfEncoder encoder : encoders.values()) {
                encoder.close();
            }
            return null;
        }
    }

    /**
     * Returns {@code true} when the write index of {@code ia} uses a routing strategy that the
     * batch path supports. Returns {@code false} for
     * {@link IndexRouting.ExtractFromSource.ForRoutingPath}, which needs dimension fields extracted
     * from the source on the shard side (unlike
     * {@link IndexRouting.ExtractFromSource.ForIndexDimensions}, which computes the {@code _tsid}
     * during columnar routing on the coordinating node and does not require source access at
     * the shard).
     */
    private static boolean isBatchRoutingCompatible(IndexAbstraction ia, ProjectMetadata project) {
        Index writeIndex = ia.getWriteIndex();
        if (writeIndex == null) {
            return true; // no write index means null batchKey, handled separately
        }
        IndexMetadata indexMeta = project.index(writeIndex);
        if (indexMeta == null) {
            return true; // will fail later in the routing pass anyway
        }
        IndexRouting routing = IndexRouting.fromIndexMetadata(indexMeta);
        // ForRoutingPath requires source-field extraction at the shard to compute _tsid.
        // ForIndexDimensions computes _tsid during columnar routing on the coordinator; it is supported.
        if (routing instanceof IndexRouting.ExtractFromSource
            && routing instanceof IndexRouting.ExtractFromSource.ForIndexDimensions == false) {
            return false;
        }
        return true;
    }
}
