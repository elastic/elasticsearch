/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.action.InferTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.results.ErrorInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.PyTorchPassThroughResults;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class TransportInferTrainedModelDeploymentActionTests extends ESTestCase {

    public void testOrderedListener() {
        int totalNumberOfResponses = 10;
        var count = new AtomicInteger();
        var results = new AtomicArray<InferenceResults>(totalNumberOfResponses);

        var exceptionHolder = new AtomicReference<Exception>();
        var resultsHolder = new AtomicReference<InferTrainedModelDeploymentAction.Response>();
        ActionListener<InferTrainedModelDeploymentAction.Response> finalListener = ActionListener.wrap(
            resultsHolder::set,
            exceptionHolder::set
        );

        List<Tuple<Integer, ActionListener<InferenceResults>>> orderedListeners = new ArrayList<>();
        for (int i = 0; i < totalNumberOfResponses; i++) {
            orderedListeners.add(
                new Tuple<>(
                    i,
                    TransportInferTrainedModelDeploymentAction.orderedListener(count, results, i, totalNumberOfResponses, finalListener)
                )
            );
        }

        // shuffle the listeners and call them in a random order
        // with a value that matches their position
        Collections.shuffle(orderedListeners, random());
        for (int i = 0; i < totalNumberOfResponses; i++) {
            int position = orderedListeners.get(i).v1();
            orderedListeners.get(i).v2().onResponse(new PyTorchPassThroughResults("foo", new double[][] { { (double) position } }, false));
        }

        // the final listener should have been called
        assertNotNull(resultsHolder.get());
        assertNull(exceptionHolder.get());

        var finalResponse = resultsHolder.get();
        assertThat(finalResponse.getResults(), hasSize(totalNumberOfResponses));
        for (int i = 0; i < totalNumberOfResponses; i++) {
            var result = (PyTorchPassThroughResults) finalResponse.getResults().get(i);
            assertEquals((double) i, result.getInference()[0][0], 0.0001);
        }
    }

    public void testOrderedListenerWithFailures() {
        int totalNumberOfResponses = 5;
        var count = new AtomicInteger();
        var results = new AtomicArray<InferenceResults>(totalNumberOfResponses);

        var exceptionHolder = new AtomicReference<Exception>();
        var resultsHolder = new AtomicReference<InferTrainedModelDeploymentAction.Response>();
        ActionListener<InferTrainedModelDeploymentAction.Response> finalListener = ActionListener.wrap(
            resultsHolder::set,
            exceptionHolder::set
        );

        // fail the first listener
        TransportInferTrainedModelDeploymentAction.orderedListener(count, results, 0, totalNumberOfResponses, finalListener)
            .onFailure(new ElasticsearchException("bad news"));

        for (int i = 1; i < totalNumberOfResponses; i++) {
            TransportInferTrainedModelDeploymentAction.orderedListener(count, results, i, totalNumberOfResponses, finalListener)
                .onResponse(new PyTorchPassThroughResults("foo", new double[][] { { (double) i } }, false));
        }

        var finalResponse = resultsHolder.get();
        assertThat(finalResponse.getResults(), hasSize(totalNumberOfResponses));
        // first response is an error
        assertThat(finalResponse.getResults().get(0), instanceOf(ErrorInferenceResults.class));
        var error = (ErrorInferenceResults) finalResponse.getResults().get(0);
        assertThat(error.getException().getMessage(), containsString("bad news"));

        for (int i = 1; i < totalNumberOfResponses; i++) {
            assertThat(finalResponse.getResults().get(i), instanceOf(PyTorchPassThroughResults.class));
        }
    }
}
