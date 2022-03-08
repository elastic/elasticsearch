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
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.Tokenization;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizer;
import org.junit.After;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizerTests.TEST_CASED_VOCAB;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;

public class BertTokenizationResultTests extends ESTestCase {

    private BertTokenizer tokenizer;

    @After
    public void closeIt() {
        if (tokenizer != null) {
            tokenizer.close();
        }
    }

    public void testBuildRequest() throws IOException {
        tokenizer = BertTokenizer.builder(TEST_CASED_VOCAB, new BertTokenization(null, null, 512, null, null)).build();

        var requestBuilder = tokenizer.requestBuilder();
        NlpTask.Request request = requestBuilder.buildRequest(List.of("Elasticsearch fun"), "request1", Tokenization.Truncate.NONE, -1);
        Map<String, Object> jsonDocAsMap = XContentHelper.convertToMap(request.processInput(), true, XContentType.JSON).v2();

        assertThat(jsonDocAsMap.keySet(), hasSize(5));
        assertEquals("request1", jsonDocAsMap.get("request_id"));
        assertEquals(Arrays.asList(12, 0, 1, 3, 13), firstListItemFromMap("tokens", jsonDocAsMap));
        assertEquals(Arrays.asList(1, 1, 1, 1, 1), firstListItemFromMap("arg_1", jsonDocAsMap));
        assertEquals(Arrays.asList(0, 0, 0, 0, 0), firstListItemFromMap("arg_2", jsonDocAsMap));
        assertEquals(Arrays.asList(0, 1, 2, 3, 4), firstListItemFromMap("arg_3", jsonDocAsMap));
    }

    private List<Integer> firstListItemFromMap(String name, Map<String, Object> jsonDocAsMap) {
        return nthListItemFromMap(name, 0, jsonDocAsMap);
    }

    @SuppressWarnings("unchecked")
    public static List<Integer> nthListItemFromMap(String name, int n, Map<String, Object> jsonDocAsMap) {
        return ((List<List<Integer>>) jsonDocAsMap.get(name)).get(n);
    }

    public void testInputTooLarge() throws IOException {
        tokenizer = BertTokenizer.builder(TEST_CASED_VOCAB, new BertTokenization(null, null, 5, null, null)).build();
        {
            var requestBuilder = tokenizer.requestBuilder();
            ElasticsearchStatusException e = expectThrows(
                ElasticsearchStatusException.class,
                () -> requestBuilder.buildRequest(
                    Collections.singletonList("Elasticsearch fun Elasticsearch fun Elasticsearch fun"),
                    "request1",
                    Tokenization.Truncate.NONE,
                    -1
                )
            );

            assertThat(
                e.getMessage(),
                containsString("Input too large. The tokenized input length [11] exceeds the maximum sequence length [5]")
            );
        }
        {
            var requestBuilder = tokenizer.requestBuilder();
            // input will become 3 tokens + the Class and Separator token = 5 which is
            // our max sequence length
            requestBuilder.buildRequest(Collections.singletonList("Elasticsearch fun"), "request1", Tokenization.Truncate.NONE, -1);
        }
    }

    @SuppressWarnings("unchecked")
    public void testBatchWithPadding() throws IOException {
        tokenizer = BertTokenizer.builder(TEST_CASED_VOCAB, new BertTokenization(null, null, 512, null, null)).build();

        var requestBuilder = tokenizer.requestBuilder();
        NlpTask.Request request = requestBuilder.buildRequest(
            List.of("Elasticsearch", "my little red car", "Godzilla day"),
            "request1",
            Tokenization.Truncate.NONE,
            -1
        );
        Map<String, Object> jsonDocAsMap = XContentHelper.convertToMap(request.processInput(), true, XContentType.JSON).v2();

        assertThat(jsonDocAsMap.keySet(), hasSize(5));
        assertThat((List<List<Integer>>) jsonDocAsMap.get("tokens"), hasSize(3));
        assertThat((List<List<Integer>>) jsonDocAsMap.get("arg_1"), hasSize(3));
        assertThat((List<List<Integer>>) jsonDocAsMap.get("arg_2"), hasSize(3));
        assertThat((List<List<Integer>>) jsonDocAsMap.get("arg_3"), hasSize(3));

        assertEquals("request1", jsonDocAsMap.get("request_id"));
        assertEquals(Arrays.asList(12, 0, 1, 13, 19, 19), nthListItemFromMap("tokens", 0, jsonDocAsMap));
        assertEquals(Arrays.asList(1, 1, 1, 1, 19, 19), nthListItemFromMap("arg_1", 0, jsonDocAsMap));
        assertEquals(Arrays.asList(0, 0, 0, 0, 0, 0), nthListItemFromMap("arg_2", 0, jsonDocAsMap));
        assertEquals(Arrays.asList(0, 1, 2, 3, 4, 5), nthListItemFromMap("arg_3", 0, jsonDocAsMap));

        assertEquals(Arrays.asList(12, 4, 5, 6, 7, 13), nthListItemFromMap("tokens", 1, jsonDocAsMap));
        assertEquals(Arrays.asList(1, 1, 1, 1, 1, 1), nthListItemFromMap("arg_1", 1, jsonDocAsMap));
        assertEquals(Arrays.asList(0, 0, 0, 0, 0, 0), nthListItemFromMap("arg_2", 1, jsonDocAsMap));
        assertEquals(Arrays.asList(0, 1, 2, 3, 4, 5), nthListItemFromMap("arg_3", 1, jsonDocAsMap));

        assertEquals(Arrays.asList(12, 8, 9, 16, 13, 19), nthListItemFromMap("tokens", 2, jsonDocAsMap));
        assertEquals(Arrays.asList(1, 1, 1, 1, 1, 19), nthListItemFromMap("arg_1", 2, jsonDocAsMap));
        assertEquals(Arrays.asList(0, 0, 0, 0, 0, 0), nthListItemFromMap("arg_2", 2, jsonDocAsMap));
        assertEquals(Arrays.asList(0, 1, 2, 3, 4, 5), nthListItemFromMap("arg_3", 2, jsonDocAsMap));
    }
}
