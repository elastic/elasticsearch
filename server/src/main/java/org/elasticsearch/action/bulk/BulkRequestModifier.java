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
import org.apache.lucene.util.SparseFixedBitSet;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.ingest.IngestService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

/**
 * Manages mutations to a bulk request that arise from the application of ingest pipelines. The modifier acts as an iterator over the
 * documents of a bulk request, keeping a record of all dropped and failed write requests in the overall bulk operation.
 * Once all pipelines have been applied, the modifier is used to create a new bulk request that will be used for executing the
 * remaining writes. When this final bulk operation is completed, the modifier is used to combine the results with those from the
 * ingest service to create the final bulk response.
 */
final class BulkRequestModifier implements Iterator<DocWriteRequest<?>> {

    private static final Logger logger = LogManager.getLogger(BulkRequestModifier.class);

    private static final String DROPPED_OR_FAILED_ITEM_WITH_AUTO_GENERATED_ID = "auto-generated";

    final BulkRequest bulkRequest;
    final SparseFixedBitSet failedSlots;
    final List<BulkItemResponse> itemResponses;
    final AtomicIntegerArray originalSlots;
    final FailureStoreDocumentConverter failureStoreDocumentConverter;

    volatile int currentSlot = -1;

    BulkRequestModifier(BulkRequest bulkRequest) {
        this.bulkRequest = bulkRequest;
        this.failedSlots = new SparseFixedBitSet(bulkRequest.requests().size());
        this.itemResponses = new ArrayList<>(bulkRequest.requests().size());
        this.originalSlots = new AtomicIntegerArray(bulkRequest.requests().size()); // oversize, but that's ok
        this.failureStoreDocumentConverter = new FailureStoreDocumentConverter();
    }

    @Override
    public DocWriteRequest<?> next() {
        return bulkRequest.requests().get(++currentSlot);
    }

    @Override
    public boolean hasNext() {
        return (currentSlot + 1) < bulkRequest.requests().size();
    }

    /**
     * Creates a new bulk request containing all documents from the original bulk request that have not been marked as failed
     * or dropped. Any failed or dropped documents are tracked as a side effect of this call so that they may be reflected in the
     * final bulk response.
     *
     * @return A new bulk request without the write operations removed during any ingest pipeline executions.
     */
    BulkRequest getBulkRequest() {
        if (itemResponses.isEmpty()) {
            return bulkRequest;
        } else {
            BulkRequest modifiedBulkRequest = bulkRequest.shallowClone();

            int slot = 0;
            List<DocWriteRequest<?>> requests = bulkRequest.requests();
            for (int i = 0; i < requests.size(); i++) {
                DocWriteRequest<?> request = requests.get(i);
                if (failedSlots.get(i) == false) {
                    modifiedBulkRequest.add(request);
                    originalSlots.set(slot++, i);
                }
            }
            return modifiedBulkRequest;
        }
    }

    /**
     * If documents were dropped or failed in ingest, this method wraps the action listener that will be notified when the
     * updated bulk operation is completed. The wrapped listener combines the dropped and failed document results from the ingest
     * service with the results returned from running the remaining write operations.
     *
     * @param ingestTookInMillis Time elapsed for ingestion to be passed to final result.
     * @param actionListener The action listener that expects the final bulk response.
     * @return An action listener that combines ingest failure results with the results from writing the remaining documents.
     */
    ActionListener<BulkResponse> wrapActionListenerIfNeeded(long ingestTookInMillis, ActionListener<BulkResponse> actionListener) {
        if (itemResponses.isEmpty()) {
            return actionListener.map(
                response -> new BulkResponse(
                    response.getItems(),
                    response.getTook().getMillis(),
                    ingestTookInMillis,
                    response.getIncrementalState()
                )
            );
        } else {
            return actionListener.map(response -> {
                // these items are the responses from the subsequent bulk request, their 'slots'
                // are not correct for this response we're building
                final BulkItemResponse[] bulkResponses = response.getItems();

                final BulkItemResponse[] allResponses = new BulkItemResponse[bulkResponses.length + itemResponses.size()];

                // the item responses are from the original request, so their slots are correct.
                // these are the responses for requests that failed early and were not passed on to the subsequent bulk.
                for (BulkItemResponse item : itemResponses) {
                    allResponses[item.getItemId()] = item;
                }

                // use the original slots for the responses from the bulk
                for (int i = 0; i < bulkResponses.length; i++) {
                    allResponses[originalSlots.get(i)] = bulkResponses[i];
                }

                if (Assertions.ENABLED) {
                    assertResponsesAreCorrect(bulkResponses, allResponses);
                }

                return new BulkResponse(allResponses, response.getTook().getMillis(), ingestTookInMillis, response.getIncrementalState());
            });
        }
    }

    private void assertResponsesAreCorrect(BulkItemResponse[] bulkResponses, BulkItemResponse[] allResponses) {
        // check for an empty intersection between the ids
        final Set<Integer> failedIds = itemResponses.stream().map(BulkItemResponse::getItemId).collect(Collectors.toSet());
        final Set<Integer> responseIds = IntStream.range(0, bulkResponses.length)
            .map(originalSlots::get) // resolve subsequent bulk ids back to the original slots
            .boxed()
            .collect(Collectors.toSet());
        assert Sets.haveEmptyIntersection(failedIds, responseIds)
            : "bulk item response slots cannot have failed and been processed in the subsequent bulk request, failed ids: "
                + failedIds
                + ", response ids: "
                + responseIds;

        // check for the correct number of responses
        final int expectedResponseCount = bulkRequest.requests.size();
        final int actualResponseCount = failedIds.size() + responseIds.size();
        assert expectedResponseCount == actualResponseCount
            : "Expected [" + expectedResponseCount + "] responses, but found [" + actualResponseCount + "]";

        // check that every response is present
        for (int i = 0; i < allResponses.length; i++) {
            assert allResponses[i] != null : "BulkItemResponse at index [" + i + "] was null";
        }
    }

    /**
     * Mark the document at the given slot in the bulk request as having failed in the ingest service.
     * @param slot the slot in the bulk request to mark as failed.
     * @param e the failure encountered.
     */
    synchronized void markItemAsFailed(int slot, Exception e, IndexDocFailureStoreStatus failureStoreStatus) {
        final DocWriteRequest<?> docWriteRequest = bulkRequest.requests().get(slot);
        final String id = Objects.requireNonNullElse(docWriteRequest.id(), DROPPED_OR_FAILED_ITEM_WITH_AUTO_GENERATED_ID);
        // We hit a error during preprocessing a request, so we:
        // 1) Remember the request item slot from the bulk, so that when we're done processing all requests we know what failed
        // 2) Add a bulk item failure for this request
        // 3) Continue with the next request in the bulk.
        failedSlots.set(slot);
        BulkItemResponse.Failure failure = new BulkItemResponse.Failure(docWriteRequest.index(), id, e, failureStoreStatus);
        itemResponses.add(BulkItemResponse.failure(slot, docWriteRequest.opType(), failure));
    }

    /**
     * Mark the document at the given slot in the bulk request as having been dropped by the ingest service.
     * @param slot the slot in the bulk request to mark as dropped.
     */
    synchronized void markItemAsDropped(int slot) {
        final DocWriteRequest<?> docWriteRequest = bulkRequest.requests().get(slot);
        final String id = Objects.requireNonNullElse(docWriteRequest.id(), DROPPED_OR_FAILED_ITEM_WITH_AUTO_GENERATED_ID);
        failedSlots.set(slot);
        UpdateResponse dropped = new UpdateResponse(
            new ShardId(docWriteRequest.index(), IndexMetadata.INDEX_UUID_NA_VALUE, 0),
            id,
            UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM,
            docWriteRequest.version(),
            DocWriteResponse.Result.NOOP
        );
        itemResponses.add(BulkItemResponse.success(slot, docWriteRequest.opType(), dropped));
    }

    /**
     * Mark the document at the given slot in the bulk request as having failed in the ingest service. The document will be redirected
     * to a data stream's failure store.
     * @param slot the slot in the bulk request to redirect.
     * @param targetIndexName the index that the document was targeting at the time of failure.
     * @param e the failure encountered.
     */
    public void markItemForFailureStore(int slot, String targetIndexName, Exception e) {
        // We get the index write request to find the source of the failed document
        IndexRequest indexRequest = TransportBulkAction.getIndexWriteRequest(bulkRequest.requests().get(slot));
        if (indexRequest == null) {
            // This is unlikely to happen ever since only source oriented operations (index, create, upsert) are considered for
            // ingest, but if it does happen, attempt to trip an assertion. If running in production, be defensive: Mark it failed
            // as normal, and log the info for later debugging if needed.
            assert false
                : "Attempting to mark invalid write request type for failure store. Only IndexRequest or UpdateRequest allowed. "
                    + "type: ["
                    + bulkRequest.requests().get(slot).getClass().getName()
                    + "], index: ["
                    + targetIndexName
                    + "]";
            markItemAsFailed(slot, e, IndexDocFailureStoreStatus.NOT_APPLICABLE_OR_UNKNOWN);
            logger.debug(
                () -> "Attempted to redirect an invalid write operation after ingest failure - type: ["
                    + bulkRequest.requests().get(slot).getClass().getName()
                    + "], index: ["
                    + targetIndexName
                    + "]"
            );
        } else {
            try {
                IndexRequest errorDocument = failureStoreDocumentConverter.transformFailedRequest(indexRequest, e, targetIndexName);
                // This is a fresh index request! We need to do some preprocessing on it. If we do not, when this is returned to
                // the bulk action, the action will see that it hasn't been processed by ingest yet and attempt to ingest it again.
                errorDocument.isPipelineResolved(true);
                errorDocument.setPipeline(IngestService.NOOP_PIPELINE_NAME);
                errorDocument.setFinalPipeline(IngestService.NOOP_PIPELINE_NAME);
                bulkRequest.requests.set(slot, errorDocument);
            } catch (IOException ioException) {
                // This is unlikely to happen because the conversion is so simple, but be defensive and attempt to report about it
                // if we need the info later.
                e.addSuppressed(ioException); // Prefer to return the original exception to the end user instead of this new one.
                logger.debug(
                    () -> "Encountered exception while attempting to redirect a failed ingest operation: index ["
                        + targetIndexName
                        + "], source: ["
                        + indexRequest.source().utf8ToString()
                        + "]",
                    ioException
                );
                markItemAsFailed(slot, e, IndexDocFailureStoreStatus.FAILED);
            }
        }
    }
}
