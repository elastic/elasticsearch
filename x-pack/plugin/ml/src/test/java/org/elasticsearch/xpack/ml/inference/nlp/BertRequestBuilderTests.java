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
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.BertTokenizationParams;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;

public class BertRequestBuilderTests extends ESTestCase {

    public void testBuildRequest() throws IOException {
        BertTokenizer tokenizer = BertTokenizer.builder(
            Arrays.asList("Elastic", "##search", "fun", BertTokenizer.CLASS_TOKEN, BertTokenizer.SEPARATOR_TOKEN),
            new BertTokenizationParams(null, null, 512)
        ).build();

        BertRequestBuilder requestBuilder = new BertRequestBuilder(tokenizer);
        NlpTask.Request request = requestBuilder.buildRequest("Elasticsearch fun", "request1");

        Map<String, Object> jsonDocAsMap = XContentHelper.convertToMap(request.processInput, true, XContentType.JSON).v2();

        assertThat(jsonDocAsMap.keySet(), hasSize(5));
        assertEquals("request1", jsonDocAsMap.get("request_id"));
        assertEquals(Arrays.asList(3, 0, 1, 2, 4), jsonDocAsMap.get("tokens"));
        assertEquals(Arrays.asList(1, 1, 1, 1, 1), jsonDocAsMap.get("arg_1"));
        assertEquals(Arrays.asList(0, 0, 0, 0, 0), jsonDocAsMap.get("arg_2"));
        assertEquals(Arrays.asList(0, 1, 2, 3, 4), jsonDocAsMap.get("arg_3"));
    }

    public void testInputTooLarge() throws IOException {
        BertTokenizer tokenizer = BertTokenizer.builder(
            Arrays.asList("Elastic", "##search", "fun", BertTokenizer.CLASS_TOKEN, BertTokenizer.SEPARATOR_TOKEN),
            new BertTokenizationParams(null, null, 5)
        ).build();
        {
            BertRequestBuilder requestBuilder = new BertRequestBuilder(tokenizer);
            ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
                () -> requestBuilder.buildRequest("Elasticsearch fun Elasticsearch fun Elasticsearch fun", "request1"));

            assertThat(e.getMessage(),
                containsString("Input too large. The tokenized input length [11] exceeds the maximum sequence length [5]"));
        }
        {
            BertRequestBuilder requestBuilder = new BertRequestBuilder(tokenizer);
            // input will become 3 tokens + the Class and Separator token = 5 which is
            // our max sequence length
            requestBuilder.buildRequest("Elasticsearch fun", "request1");
        }
    }
}
