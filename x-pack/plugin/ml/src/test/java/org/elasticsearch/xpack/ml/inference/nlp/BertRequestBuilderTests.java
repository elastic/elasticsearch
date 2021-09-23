/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.BertTokenization;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;

public class BertRequestBuilderTests extends ESTestCase {

    public void testBuildRequest() throws IOException {
        BertTokenizer tokenizer = BertTokenizer.builder(
            Arrays.asList("Elastic", "##search", "fun", BertTokenizer.CLASS_TOKEN, BertTokenizer.SEPARATOR_TOKEN, BertTokenizer.PAD_TOKEN),
            new BertTokenization(null, null, 512)
        ).build();

        BertRequestBuilder requestBuilder = new BertRequestBuilder(tokenizer);
        NlpTask.Request request = requestBuilder.buildRequest(List.of("Elasticsearch fun"), "request1");
        Map<String, Object> jsonDocAsMap = XContentHelper.convertToMap(request.processInput, true, XContentType.JSON).v2();

        assertThat(jsonDocAsMap.keySet(), hasSize(5));
        assertEquals("request1", jsonDocAsMap.get("request_id"));
        assertEquals(Arrays.asList(3, 0, 1, 2, 4), firstListItemFromMap("tokens", jsonDocAsMap));
        assertEquals(Arrays.asList(1, 1, 1, 1, 1), firstListItemFromMap("arg_1", jsonDocAsMap));
        assertEquals(Arrays.asList(0, 0, 0, 0, 0), firstListItemFromMap("arg_2", jsonDocAsMap));
        assertEquals(Arrays.asList(0, 1, 2, 3, 4), firstListItemFromMap("arg_3", jsonDocAsMap));
    }

    @SuppressWarnings("unchecked")
    private List<Integer> firstListItemFromMap(String name, Map<String, Object> jsonDocAsMap) {
        return nthListItemFromMap(name, 0, jsonDocAsMap);
    }

    @SuppressWarnings("unchecked")
    public static List<Integer> nthListItemFromMap(String name, int n, Map<String, Object> jsonDocAsMap) {
        return ((List<List<Integer>>)jsonDocAsMap.get(name)).get(n);
    }

    public void testInputTooLarge() throws IOException {
        BertTokenizer tokenizer = BertTokenizer.builder(
            Arrays.asList("Elastic", "##search", "fun", BertTokenizer.CLASS_TOKEN, BertTokenizer.SEPARATOR_TOKEN, BertTokenizer.PAD_TOKEN),
            new BertTokenization(null, null, 5)
        ).build();
        {
            BertRequestBuilder requestBuilder = new BertRequestBuilder(tokenizer);
            ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
                () -> requestBuilder.buildRequest(Collections.singletonList("Elasticsearch fun Elasticsearch fun Elasticsearch fun"),
                    "request1"));

            assertThat(e.getMessage(),
                containsString("Input too large. The tokenized input length [11] exceeds the maximum sequence length [5]"));
        }
        {
            BertRequestBuilder requestBuilder = new BertRequestBuilder(tokenizer);
            // input will become 3 tokens + the Class and Separator token = 5 which is
            // our max sequence length
            requestBuilder.buildRequest(Collections.singletonList("Elasticsearch fun"), "request1");
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

        BertRequestBuilder requestBuilder = new BertRequestBuilder(tokenizer);
        NlpTask.Request request = requestBuilder.buildRequest(
            List.of("Elasticsearch",
                "my little red car",
                "Godzilla day"), "request1");
        Map<String, Object> jsonDocAsMap = XContentHelper.convertToMap(request.processInput, true, XContentType.JSON).v2();

        assertThat(jsonDocAsMap.keySet(), hasSize(5));
        assertThat((List<List<Integer>>) jsonDocAsMap.get("tokens"), hasSize(3));
        assertThat((List<List<Integer>>) jsonDocAsMap.get("arg_1"), hasSize(3));
        assertThat((List<List<Integer>>) jsonDocAsMap.get("arg_2"), hasSize(3));
        assertThat((List<List<Integer>>) jsonDocAsMap.get("arg_3"), hasSize(3));

        assertEquals("request1", jsonDocAsMap.get("request_id"));
        assertEquals(Arrays.asList(1, 3, 4, 2, 0, 0), nthListItemFromMap("tokens", 0, jsonDocAsMap));
        assertEquals(Arrays.asList(1, 1, 1, 1, 0, 0), nthListItemFromMap("arg_1", 0, jsonDocAsMap));
        assertEquals(Arrays.asList(0, 0, 0, 0, 0, 0), nthListItemFromMap("arg_2", 0, jsonDocAsMap));
        assertEquals(Arrays.asList(0, 1, 2, 3, 4, 5), nthListItemFromMap("arg_3", 0, jsonDocAsMap));

        assertEquals(Arrays.asList(1, 8, 9, 10, 11, 2), nthListItemFromMap("tokens", 1, jsonDocAsMap));
        assertEquals(Arrays.asList(1, 1, 1, 1, 1, 1), nthListItemFromMap("arg_1", 1, jsonDocAsMap));
        assertEquals(Arrays.asList(0, 0, 0, 0, 0, 0), nthListItemFromMap("arg_2", 1, jsonDocAsMap));
        assertEquals(Arrays.asList(0, 1, 2, 3, 4, 5), nthListItemFromMap("arg_3", 1, jsonDocAsMap));

        assertEquals(Arrays.asList(1, 12, 13, 7, 2, 0), nthListItemFromMap("tokens", 2, jsonDocAsMap));
        assertEquals(Arrays.asList(1, 1, 1, 1, 1, 0), nthListItemFromMap("arg_1", 2, jsonDocAsMap));
        assertEquals(Arrays.asList(0, 0, 0, 0, 0, 0), nthListItemFromMap("arg_2", 2, jsonDocAsMap));
        assertEquals(Arrays.asList(0, 1, 2, 3, 4, 5), nthListItemFromMap("arg_3", 2, jsonDocAsMap));
    }
}
