/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pipelines.nlp;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xpack.ml.inference.pipelines.nlp.tokenizers.BertTokenizer;

import java.io.IOException;

public class NlpPipeline {

    private final TaskType taskType;
    private final BertTokenizer tokenizer;

    private NlpPipeline(TaskType taskType, BertTokenizer tokenizer) {
        this.taskType = taskType;
        this.tokenizer = tokenizer;
    }

    public BytesReference createRequest(String inputs, String requestId) throws IOException {
        BertTokenizer.TokenizationResult tokens = tokenizer.tokenize(inputs);
        return taskType.jsonRequest(tokens.getTokenIds(), requestId);
    }

    public static NlpPipeline fromConfig(PipelineConfig config) {
        return new NlpPipeline(config.getTaskType(), config.buildTokenizer());
    }
}
