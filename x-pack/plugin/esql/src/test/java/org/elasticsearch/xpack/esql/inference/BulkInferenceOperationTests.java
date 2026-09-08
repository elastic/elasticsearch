/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceOperation;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceRequestItem;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceRequestItemIterator;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceResponseItem;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;

/**
 * Exercises {@link BulkInferenceOperation#onToleratedInferenceFailure} directly. Whether a tolerated failure registers its
 * warning depends on its ordering against concurrent inference callbacks, an ordering a driver-level test cannot stage
 * deterministically.
 */
public class BulkInferenceOperationTests extends ESTestCase {

    /**
     * A tolerated failure arriving while the operation is still running registers its warning and completes its request with a
     * null response, leaving the operation open for its remaining requests.
     */
    public void testToleratedFailureRegistersWarningWhileRunning() {
        PlainActionFuture<List<BulkInferenceResponseItem>> completion = new PlainActionFuture<>();
        BulkInferenceOperation operation = new BulkInferenceOperation(requestIterator(2), completion);

        BulkInferenceRequestItem request = operation.pollNextRequest();
        operation.pollNextRequest();

        AtomicBoolean warned = new AtomicBoolean(false);
        operation.onToleratedInferenceFailure(request.createResponse(null), () -> warned.set(true));

        assertThat(warned.get(), equalTo(true));
        assertFalse("operation still has an outstanding request", completion.isDone());
    }

    /**
     * A failed operation has already fired its completion listener, which releases the driver to finish and snapshot its
     * warnings. A tolerated failure arriving after that registers no warning, and the operation's own failure is what surfaces.
     */
    public void testToleratedFailureAfterOperationFailedIsDropped() {
        PlainActionFuture<List<BulkInferenceResponseItem>> completion = new PlainActionFuture<>();
        BulkInferenceOperation operation = new BulkInferenceOperation(requestIterator(2), completion);

        operation.pollNextRequest();
        BulkInferenceRequestItem inFlight = operation.pollNextRequest();

        operation.onException(new ElasticsearchException("bulk operation failed"));

        AtomicBoolean warned = new AtomicBoolean(false);
        operation.onToleratedInferenceFailure(inFlight.createResponse(null), () -> warned.set(true));

        assertThat(warned.get(), equalTo(false));
        ElasticsearchException failure = expectThrows(ElasticsearchException.class, completion::actionGet);
        assertThat(failure.getMessage(), equalTo("bulk operation failed"));
    }

    private static BulkInferenceRequestItemIterator requestIterator(int size) {
        return new BulkInferenceRequestItemIterator() {
            private int remaining = size;

            @Override
            public int estimatedSize() {
                return size;
            }

            @Override
            public boolean hasNext() {
                return remaining > 0;
            }

            @Override
            public BulkInferenceRequestItem next() {
                remaining--;
                // The request is null because these items are only sequenced and completed here, never dispatched.
                return new BulkInferenceRequestItem(null, BulkInferenceRequestItem.SINGLE_ONE_POSITION_VALUE_COUNTS, -1);
            }

            @Override
            public void close() {}
        };
    }
}
