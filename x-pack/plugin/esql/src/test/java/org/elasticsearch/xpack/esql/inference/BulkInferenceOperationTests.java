/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceOperation;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceRequestItem;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceRequestItemIterator;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceResponseItem;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

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

    /**
     * The responses handed to the completion listener must not be emptied afterwards by the operation that produced them.
     *
     * The list is published by reference: {@code completeIfFinished} passes {@code Collections.unmodifiableList(responses)},
     * which is a view over the live {@code ArrayList} rather than a copy, and nothing downstream copies it either --
     * {@code InferenceOperator#performAsync} maps it straight into an {@code OngoingInferenceResult} that
     * {@code InferenceOperator#getOutput} reads on a later turn of the driver. In between, {@code clearBuffers} empties that
     * same list whenever {@code hasFailure()} is true.
     *
     * A failure can arrive in exactly that window because {@code onException} sets the flag WITHOUT holding the checkpoint
     * lock, so {@code hasFailure()} can flip between the success decision and {@code clearBuffers()} a statement later. The
     * listener below stands in for that concurrent caller: it runs inside the window by construction, which makes an
     * interleaving that is otherwise timing-dependent deterministic here.
     *
     * The consequence downstream is not a lost response but a corrupt page: the embedding output builder appends one entry
     * per response, so an emptied list yields a block with zero positions and the page invariant fails with
     * "does not have same position count: 0 != N" -- naming a block type and two numbers, and nothing about inference.
     */
    public void testResponsesHandedToTheListenerSurviveALateFailure() {
        AtomicReference<List<BulkInferenceResponseItem>> delivered = new AtomicReference<>();
        AtomicReference<BulkInferenceOperation> operationRef = new AtomicReference<>();

        ActionListener<List<BulkInferenceResponseItem>> listener = ActionListener.wrap(responses -> {
            delivered.set(responses);
            operationRef.get().onException(new ElasticsearchException("failure arriving after the handoff"));
        }, e -> fail("the operation completed successfully, so the listener must not see a failure: " + e));

        BulkInferenceOperation operation = new BulkInferenceOperation(requestIterator(1), listener);
        operationRef.set(operation);

        BulkInferenceRequestItem request = operation.pollNextRequest();
        operation.onInferenceResponse(request.createResponse(null));

        assertThat("the operation completed, so the listener was handed its responses", delivered.get(), notNullValue());
        assertThat(
            "the one response handed over is still there; the producer must not empty a list it has published",
            delivered.get(),
            hasSize(1)
        );
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
