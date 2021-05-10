/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pipelines.nlp;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xpack.core.ml.inference.deployment.PyTorchResult;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.ml.inference.pipelines.nlp.tokenizers.BertTokenizer;

import java.io.IOException;

public class NlpPipeline {

    private final TaskType taskType;
    private final BertTokenizer tokenizer;

    private NlpPipeline(TaskType taskType, BertTokenizer tokenizer) {
        this.taskType = taskType;
        this.tokenizer = tokenizer;
    }

    public Processor createProcessor() throws IOException {
        return taskType.createProcessor(tokenizer);
    }

    public static NlpPipeline fromConfig(PipelineConfig config) {
        return new NlpPipeline(config.getTaskType(), config.buildTokenizer());
    }

    public interface RequestBuilder {
        BytesReference buildRequest(String inputs, String requestId) throws IOException;
    }

    public interface ResultProcessor {
        InferenceResults processResult(PyTorchResult pyTorchResult);
    }


    public abstract static class Processor {

        protected static final String REQUEST_ID = "request_id";
        protected static final String TOKENS = "tokens";
        protected static final String ARG1 = "arg_1";
        protected static final String ARG2 = "arg_2";
        protected static final String ARG3 = "arg_3";

        public abstract RequestBuilder getRequestBuilder();
        public abstract ResultProcessor getResultProcessor();
    }
}
