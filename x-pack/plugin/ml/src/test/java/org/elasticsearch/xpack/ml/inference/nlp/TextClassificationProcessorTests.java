/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.BertTokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.Tokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.VocabularyConfig;
import org.elasticsearch.xpack.ml.inference.deployment.PyTorchResult;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizer;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.NlpTokenizer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;

public class TextClassificationProcessorTests extends ESTestCase {

    public void testInvalidResult() {
        {
            PyTorchResult torchResult = new PyTorchResult("foo", new double[][][] {}, 0L, null);
            var e = expectThrows(
                ElasticsearchStatusException.class,
                () -> TextClassificationProcessor.processResult(null, torchResult, randomInt(), List.of("a", "b"), randomAlphaOfLength(10))
            );
            assertThat(e.getMessage(), containsString("Text classification result has no data"));
        }
        {
            PyTorchResult torchResult = new PyTorchResult("foo", new double[][][] { { { 1.0 } } }, 0L, null);
            var e = expectThrows(
                ElasticsearchStatusException.class,
                () -> TextClassificationProcessor.processResult(null, torchResult, randomInt(), List.of("a", "b"), randomAlphaOfLength(10))
            );
            assertThat(e.getMessage(), containsString("Expected exactly [2] values in text classification result; got [1]"));
        }
    }

    @SuppressWarnings("unchecked")
    public void testBuildRequest() throws IOException {
        NlpTokenizer tokenizer = NlpTokenizer.build(
            new Vocabulary(
                Arrays.asList(
                    "Elastic",
                    "##search",
                    "fun",
                    BertTokenizer.CLASS_TOKEN,
                    BertTokenizer.SEPARATOR_TOKEN,
                    BertTokenizer.PAD_TOKEN
                ),
                randomAlphaOfLength(10)
            ),
            new BertTokenization(null, null, 512, Tokenization.Truncate.NONE)
        );

        TextClassificationConfig config = new TextClassificationConfig(
            new VocabularyConfig("test-index"),
            null,
            List.of("a", "b"),
            null,
            null
        );

        TextClassificationProcessor processor = new TextClassificationProcessor(tokenizer, config);

        NlpTask.Request request = processor.getRequestBuilder(config).buildRequest(List.of("Elasticsearch fun"), "request1");

        Map<String, Object> jsonDocAsMap = XContentHelper.convertToMap(request.processInput, true, XContentType.JSON).v2();

        assertThat(jsonDocAsMap.keySet(), hasSize(5));
        assertEquals("request1", jsonDocAsMap.get("request_id"));
        assertEquals(Arrays.asList(3, 0, 1, 2, 4), ((List<List<Integer>>) jsonDocAsMap.get("tokens")).get(0));
        assertEquals(Arrays.asList(1, 1, 1, 1, 1), ((List<List<Integer>>) jsonDocAsMap.get("arg_1")).get(0));
    }
}
