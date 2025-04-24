/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.CountDownActionListener;
import org.elasticsearch.compute.operator.FailureCollector;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.esql.inference.InferenceRequestSupplier;
import org.elasticsearch.xpack.esql.inference.InferenceRunner;

import java.util.ArrayList;
import java.util.List;

public class BulkInferenceOperation<InferenceResults extends InferenceServiceResults, OutputType> implements Releasable {
    private final BulkInferenceRequestIterator requests;
    private final BulkInferenceOutputBuilder<InferenceResults, OutputType> outputBuilder;

    public BulkInferenceOperation(
        BulkInferenceRequestIterator requests,
        BulkInferenceOutputBuilder<InferenceResults, OutputType> outputBuilder
    ) {
        this.requests = requests;
        this.outputBuilder = outputBuilder;
    }

    public void execute(InferenceRunner inferenceRunner, ActionListener<OutputType> listener) {

        listener = listener.delegateFailureAndWrap((l, r) -> {
            this.close();
            l.onResponse(r);
        });

        // TODO: use a checkpoint instead of a CountDownActionListener
        // TODO: output when data arrive
        List<InferenceRequestSupplier> requestSuppliers = new ArrayList<>();
        while (requests.hasNext()) {
            requestSuppliers.add(requests.next());
        }

        InferenceAction.Response[] responses = new InferenceAction.Response[requestSuppliers.size()];
        CountDownActionListener countDownActionListener = new CountDownActionListener(
            requestSuppliers.size(),
            listener.delegateFailureIgnoreResponseAndWrap((l) -> {
                FailureCollector failureCollector = new FailureCollector();
                for (var response : responses) {
                    try {
                        outputBuilder.accept(response);
                    } catch (Exception e) {
                        failureCollector.unwrapAndCollect(e);
                    }
                }

                if (failureCollector.hasFailure()) {
                    throw failureCollector.getFailure();
                }

                l.onResponse(outputBuilder.buildOutput());
            })
        );

        for (int i = 0; i < requestSuppliers.size(); i++) {
            int currentIndex = i;
            inferenceRunner.doInference(requestSuppliers.get(i), countDownActionListener.delegateFailureAndWrap((l, r) -> {
                responses[currentIndex] = r;
                l.onResponse(null);
            }));
        }
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(requests, outputBuilder);
    }
}
