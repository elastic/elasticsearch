/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.BertTokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.DistilBertTokenization;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.ml.inference.nlp.BertRequestBuilderTests.nthListItemFromMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;

public class DistilBertRequestBuilderTests extends ESTestCase {

    public void testBuildRequest() throws IOException {
        BertTokenizer tokenizer = BertTokenizer.builder(
            Arrays.asList("Elastic", "##search", "fun", BertTokenizer.CLASS_TOKEN, BertTokenizer.SEPARATOR_TOKEN, BertTokenizer.PAD_TOKEN),
            new DistilBertTokenization(null, null, 512)
        ).build();

        DistilBertRequestBuilder requestBuilder = new DistilBertRequestBuilder(tokenizer);
        BytesReference bytesReference = requestBuilder.buildRequest(List.of("Elasticsearch fun"), "request1").processInput;

        Map<String, Object> jsonDocAsMap = XContentHelper.convertToMap(bytesReference, true, XContentType.JSON).v2();

        assertThat(jsonDocAsMap.keySet(), hasSize(3));
        assertEquals("request1", jsonDocAsMap.get("request_id"));
        assertEquals(Arrays.asList(3, 0, 1, 2, 4), nthListItemFromMap("tokens", 0, jsonDocAsMap));
        assertEquals(Arrays.asList(1, 1, 1, 1, 1), nthListItemFromMap("arg_1", 0, jsonDocAsMap));
    }

    public void testInputTooLarge() throws IOException {
        BertTokenizer tokenizer = BertTokenizer.builder(
            Arrays.asList("Elastic", "##search", "fun", BertTokenizer.CLASS_TOKEN, BertTokenizer.SEPARATOR_TOKEN, BertTokenizer.PAD_TOKEN),
            new DistilBertTokenization(null, null, 5)
        ).build();
        {
            DistilBertRequestBuilder requestBuilder = new DistilBertRequestBuilder(tokenizer);
            ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
                () -> requestBuilder.buildRequest(List.of("Elasticsearch fun Elasticsearch fun Elasticsearch fun"), "request1"));

            assertThat(e.getMessage(),
                containsString("Input too large. The tokenized input length [11] exceeds the maximum sequence length [5]"));
        }
        {
            DistilBertRequestBuilder requestBuilder = new DistilBertRequestBuilder(tokenizer);
            // input will become 3 tokens + the Class and Separator token = 5 which is
            // our max sequence length
            requestBuilder.buildRequest(List.of("Elasticsearch fun"), "request1");
        }
    }

    @SuppressWarnings("unchecked")
    public void testBatchWithPadding() throws IOException {
        BertTokenizer tokenizer = BertTokenizer.builder(
            Arrays.asList(BertTokenizer.PAD_TOKEN, BertTokenizer.CLASS_TOKEN, BertTokenizer.SEPARATOR_TOKEN,
                "Elastic", "##search", "fun",
                "Pancake", "day",
                "my", "little", "red", "car",
                "God", "##zilla"
            ),
            new BertTokenization(null, null, 512)
        ).build();

        DistilBertRequestBuilder requestBuilder = new DistilBertRequestBuilder(tokenizer);
        NlpTask.Request request = requestBuilder.buildRequest(
            List.of("Elasticsearch",
                "my little red car",
                "Godzilla day"), "request1");
        Map<String, Object> jsonDocAsMap = XContentHelper.convertToMap(request.processInput, true, XContentType.JSON).v2();

        assertEquals("request1", jsonDocAsMap.get("request_id"));
        assertThat(jsonDocAsMap.keySet(), hasSize(3));
        assertThat((List<List<Integer>>) jsonDocAsMap.get("tokens"), hasSize(3));
        assertThat((List<List<Integer>>) jsonDocAsMap.get("arg_1"), hasSize(3));

        assertEquals(Arrays.asList(1, 3, 4, 2, 0, 0), nthListItemFromMap("tokens", 0, jsonDocAsMap));
        assertEquals(Arrays.asList(1, 1, 1, 1, 0, 0), nthListItemFromMap("arg_1", 0, jsonDocAsMap));

        assertEquals(Arrays.asList(1, 8, 9, 10, 11, 2), nthListItemFromMap("tokens", 1, jsonDocAsMap));
        assertEquals(Arrays.asList(1, 1, 1, 1, 1, 1), nthListItemFromMap("arg_1", 1, jsonDocAsMap));

        assertEquals(Arrays.asList(1, 12, 13, 7, 2, 0), nthListItemFromMap("tokens", 2, jsonDocAsMap));
        assertEquals(Arrays.asList(1, 1, 1, 1, 1, 0), nthListItemFromMap("arg_1", 2, jsonDocAsMap));
    }
}
