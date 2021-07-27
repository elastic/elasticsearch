/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.BertTokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.SentimentAnalysisConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.VocabularyConfig;
import org.elasticsearch.xpack.ml.inference.deployment.PyTorchResult;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class SentimentAnalysisProcessorTests extends ESTestCase {

    public void testInvalidResult() {
        SentimentAnalysisConfig config = new SentimentAnalysisConfig(new VocabularyConfig("test-index", "vocab"), null, null);
        SentimentAnalysisProcessor processor = new SentimentAnalysisProcessor(mock(BertTokenizer.class), config);
        {
            PyTorchResult torchResult = new PyTorchResult("foo", new double[][][]{}, 0L, null);
            InferenceResults inferenceResults = processor.processResult(null, torchResult);
            assertThat(inferenceResults, instanceOf(WarningInferenceResults.class));
            assertEquals("Sentiment analysis result has no data",
                ((WarningInferenceResults) inferenceResults).getWarning());
        }
        {
            PyTorchResult torchResult = new PyTorchResult("foo", new double[][][]{{{1.0}}}, 0L, null);
            InferenceResults inferenceResults = processor.processResult(null, torchResult);
            assertThat(inferenceResults, instanceOf(WarningInferenceResults.class));
            assertEquals("Expected 2 values in sentiment analysis result",
                ((WarningInferenceResults)inferenceResults).getWarning());
        }
    }

    public void testBuildRequest() throws IOException {
        BertTokenizer tokenizer = BertTokenizer.builder(
            Arrays.asList("Elastic", "##search", "fun", BertTokenizer.CLASS_TOKEN, BertTokenizer.SEPARATOR_TOKEN),
            new BertTokenization(null, null, 512)
        ).build();

        SentimentAnalysisConfig config = new SentimentAnalysisConfig(new VocabularyConfig("test-index", "vocab"), null, null);
        SentimentAnalysisProcessor processor = new SentimentAnalysisProcessor(tokenizer, config);

        NlpTask.Request request = processor.buildRequest("Elasticsearch fun", "request1");

        Map<String, Object> jsonDocAsMap = XContentHelper.convertToMap(request.processInput, true, XContentType.JSON).v2();

        assertThat(jsonDocAsMap.keySet(), hasSize(3));
        assertEquals("request1", jsonDocAsMap.get("request_id"));
        assertEquals(Arrays.asList(3, 0, 1, 2, 4), jsonDocAsMap.get("tokens"));
        assertEquals(Arrays.asList(1, 1, 1, 1, 1), jsonDocAsMap.get("arg_1"));
    }

    public void testValidate() {

        SentimentAnalysisConfig config = new SentimentAnalysisConfig(new VocabularyConfig("test-index", "vocab"), null,
            List.of("too", "many", "class", "labels"));

        ValidationException validationException = expectThrows(ValidationException.class,
            () -> new SentimentAnalysisProcessor(mock(BertTokenizer.class), config));

        assertThat(validationException.getMessage(),
            containsString("Sentiment analysis requires exactly 2 [classification_labels]. Invalid labels [too, many, class, labels]"));
    }
 }
