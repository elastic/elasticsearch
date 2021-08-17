/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.PyTorchPassThroughResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.BertPassThroughConfig;
import org.elasticsearch.xpack.ml.inference.deployment.PyTorchResult;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.NlpTokenizer;

/**
 * A NLP processor that directly returns the PyTorch result
 * without any post-processing
 */
public class PassThroughProcessor implements NlpTask.Processor {

    private final NlpTask.RequestBuilder requestBuilder;

    PassThroughProcessor(NlpTokenizer tokenizer, BertPassThroughConfig config) {
        this.requestBuilder = tokenizer.requestBuilder(config, t -> PassThroughProcessor::processResult);
    }

    @Override
    public void validateInputs(String inputs) {
        // nothing to validate
    }

    @Override
    public NlpTask.RequestBuilder getRequestBuilder() {
        return requestBuilder;
    }

    private static InferenceResults processResult(PyTorchResult pyTorchResult) {
        return new PyTorchPassThroughResults(pyTorchResult.getInferenceResult());
    }
}
