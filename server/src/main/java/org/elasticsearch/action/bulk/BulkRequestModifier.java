/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.apache.lucene.util.SparseFixedBitSet;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.index.shard.ShardId;

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

final class BulkRequestModifier implements Iterator<DocWriteRequest<?>> {

    private static final String DROPPED_OR_FAILED_ITEM_WITH_AUTO_GENERATED_ID = "auto-generated";

    final BulkRequest bulkRequest;
    final SparseFixedBitSet failedSlots;
    final List<BulkItemResponse> itemResponses;
    final AtomicIntegerArray originalSlots;

    volatile int currentSlot = -1;

    BulkRequestModifier(BulkRequest bulkRequest) {
        this.bulkRequest = bulkRequest;
        this.failedSlots = new SparseFixedBitSet(bulkRequest.requests().size());
        this.itemResponses = new ArrayList<>(bulkRequest.requests().size());
        this.originalSlots = new AtomicIntegerArray(bulkRequest.requests().size()); // oversize, but that's ok
    }

    @Override
    public DocWriteRequest<?> next() {
        return bulkRequest.requests().get(++currentSlot);
    }

    @Override
    public boolean hasNext() {
        return (currentSlot + 1) < bulkRequest.requests().size();
    }

    BulkRequest getBulkRequest() {
        if (itemResponses.isEmpty()) {
            return bulkRequest;
        } else {
            BulkRequest modifiedBulkRequest = new BulkRequest();
            modifiedBulkRequest.setRefreshPolicy(bulkRequest.getRefreshPolicy());
            modifiedBulkRequest.waitForActiveShards(bulkRequest.waitForActiveShards());
            modifiedBulkRequest.timeout(bulkRequest.timeout());

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

    ActionListener<BulkResponse> wrapActionListenerIfNeeded(long ingestTookInMillis, ActionListener<BulkResponse> actionListener) {
        if (itemResponses.isEmpty()) {
            return actionListener.map(
                response -> new BulkResponse(response.getItems(), response.getTook().getMillis(), ingestTookInMillis)
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

                return new BulkResponse(allResponses, response.getTook().getMillis(), ingestTookInMillis);
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

    synchronized void markItemAsFailed(int slot, Exception e) {
        final DocWriteRequest<?> docWriteRequest = bulkRequest.requests().get(slot);
        final String id = Objects.requireNonNullElse(docWriteRequest.id(), DROPPED_OR_FAILED_ITEM_WITH_AUTO_GENERATED_ID);
        // We hit a error during preprocessing a request, so we:
        // 1) Remember the request item slot from the bulk, so that when we're done processing all requests we know what failed
        // 2) Add a bulk item failure for this request
        // 3) Continue with the next request in the bulk.
        failedSlots.set(slot);
        BulkItemResponse.Failure failure = new BulkItemResponse.Failure(docWriteRequest.index(), id, e);
        itemResponses.add(BulkItemResponse.failure(slot, docWriteRequest.opType(), failure));
    }

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
}
