/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.deployment.PyTorchResult;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizer;

import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class SentimentAnalysisProcessorTests extends ESTestCase {

    public void testInvalidResult() {
        SentimentAnalysisProcessor processor = new SentimentAnalysisProcessor(mock(BertTokenizer.class));
        {
            PyTorchResult torchResult = new PyTorchResult("foo", new double[][]{}, null);
            InferenceResults inferenceResults = processor.processResult(torchResult);
            assertThat(inferenceResults, instanceOf(WarningInferenceResults.class));
            assertEquals("Sentiment analysis result has no data",
                ((WarningInferenceResults) inferenceResults).getWarning());
        }
        {
            PyTorchResult torchResult = new PyTorchResult("foo", new double[][]{{1.0}}, null);
            InferenceResults inferenceResults = processor.processResult(torchResult);
            assertThat(inferenceResults, instanceOf(WarningInferenceResults.class));
            assertEquals("Expected 2 values in sentiment analysis result",
                ((WarningInferenceResults)inferenceResults).getWarning());
        }
    }
 }
