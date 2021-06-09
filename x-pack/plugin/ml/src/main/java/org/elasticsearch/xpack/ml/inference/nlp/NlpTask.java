/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xpack.ml.inference.deployment.PyTorchResult;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizer;

import java.io.IOException;

public class NlpTask {

    private final TaskType taskType;
    private final BertTokenizer tokenizer;

    public static NlpTask fromConfig(NlpTaskConfig config) {
        return new NlpTask(config.getTaskType(), config.buildTokenizer());
    }

    private NlpTask(TaskType taskType, BertTokenizer tokenizer) {
        this.taskType = taskType;
        this.tokenizer = tokenizer;
    }

    public Processor createProcessor() throws IOException {
        return taskType.createProcessor(tokenizer);
    }

    public interface RequestBuilder {
        BytesReference buildRequest(String inputs, String requestId) throws IOException;
    }

    public interface ResultProcessor {
        InferenceResults processResult(PyTorchResult pyTorchResult);
    }

    public interface Processor {
        /**
         * Validate the task input.
         * Throws an exception if the inputs fail validation
         *
         * @param inputs Text to validate
         */
        void validateInputs(String inputs);

        RequestBuilder getRequestBuilder();
        ResultProcessor getResultProcessor();
    }
}
