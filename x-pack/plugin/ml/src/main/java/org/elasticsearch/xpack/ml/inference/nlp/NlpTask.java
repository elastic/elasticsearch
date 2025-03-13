/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.Tokenization;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.NlpTokenizer;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.TokenizationResult;
import org.elasticsearch.xpack.ml.inference.pytorch.results.PyTorchInferenceResult;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class NlpTask {

    private final NlpConfig config;
    private final NlpTokenizer tokenizer;

    public NlpTask(NlpConfig config, Vocabulary vocabulary) throws IOException {
        this.config = config;
        this.tokenizer = NlpTokenizer.build(vocabulary, config.getTokenization());
    }

    /**
     * Create and validate the NLP Processor
     * @return the processor based on task type
     * @throws ValidationException if the validation fails
     */
    public Processor createProcessor() throws ValidationException {
        return TaskType.fromString(config.getName()).createProcessor(tokenizer, config);
    }

    public interface RequestBuilder {
        Request buildRequest(List<String> inputs, String requestId, Tokenization.Truncate truncate, int span, Integer windowSize)
            throws IOException;
    }

    public interface ResultProcessor {
        InferenceResults processResult(TokenizationResult tokenization, PyTorchInferenceResult pyTorchResult, boolean chunkResult);
    }

    public abstract static class Processor implements Releasable {

        protected final NlpTokenizer tokenizer;

        public Processor(NlpTokenizer tokenizer) {
            this.tokenizer = tokenizer;
        }

        @Override
        public void close() {
            tokenizer.close();
        }

        /**
         * Validate the task input string.
         * Throws an exception if the inputs fail validation
         *
         * @param inputs Text to validate
         */
        public abstract void validateInputs(List<String> inputs);

        public abstract RequestBuilder getRequestBuilder(NlpConfig config);

        public abstract ResultProcessor getResultProcessor(NlpConfig config);

        static ElasticsearchException chunkingNotSupportedException(TaskType taskType) {
            throw chunkingNotSupportedException(TaskType.NER);
        }
    }

    public record Request(TokenizationResult tokenization, BytesReference processInput) {
        public Request(TokenizationResult tokenization, BytesReference processInput) {
            this.tokenization = Objects.requireNonNull(tokenization);
            this.processInput = Objects.requireNonNull(processInput);
        }
    }
}
