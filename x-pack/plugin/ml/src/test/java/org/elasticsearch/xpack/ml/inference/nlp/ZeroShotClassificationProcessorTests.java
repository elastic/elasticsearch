/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.BertTokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.Tokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.VocabularyConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ZeroShotClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ZeroShotClassificationConfigUpdate;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.NlpTokenizer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizerTests.TEST_CASED_VOCAB;
import static org.hamcrest.Matchers.hasSize;

public class ZeroShotClassificationProcessorTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    public void testBuildRequest() throws IOException {
        NlpTokenizer tokenizer = NlpTokenizer.build(
            new Vocabulary(TEST_CASED_VOCAB, randomAlphaOfLength(10), List.of()),
            new BertTokenization(null, true, 512, Tokenization.Truncate.NONE, -1)
        );

        ZeroShotClassificationConfig config = new ZeroShotClassificationConfig(
            List.of("entailment", "neutral", "contradiction"),
            new VocabularyConfig("test-index"),
            null,
            null,
            null,
            null,
            null
        );
        ZeroShotClassificationProcessor processor = new ZeroShotClassificationProcessor(tokenizer, config);

        NlpTask.Request request = processor.getRequestBuilder(
            (NlpConfig) new ZeroShotClassificationConfigUpdate.Builder().setLabels(List.of("new", "stuff")).build().apply(config)
        ).buildRequest(List.of("Elasticsearch fun"), "request1", Tokenization.Truncate.NONE, -1);

        Map<String, Object> jsonDocAsMap = XContentHelper.convertToMap(request.processInput(), true, XContentType.JSON).v2();

        assertThat(jsonDocAsMap.keySet(), hasSize(5));
        assertEquals("request1", jsonDocAsMap.get("request_id"));
        assertEquals(Arrays.asList(12, 0, 1, 3, 13, 15, 15, 2, 15, 10, 13), ((List<List<Integer>>) jsonDocAsMap.get("tokens")).get(0));
        assertEquals(Arrays.asList(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ((List<List<Integer>>) jsonDocAsMap.get("arg_1")).get(0));
        assertEquals(Arrays.asList(12, 0, 1, 3, 13, 15, 15, 2, 15, 10, 13), ((List<List<Integer>>) jsonDocAsMap.get("tokens")).get(1));
        assertEquals(Arrays.asList(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), ((List<List<Integer>>) jsonDocAsMap.get("arg_1")).get(1));
    }

}
