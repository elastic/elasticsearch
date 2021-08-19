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
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.DistilBertTokenizationParams;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizer;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.DistilBertTokenizer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;

public class DistilBertRequestBuilderTests extends ESTestCase {

    public void testBuildRequest() throws IOException {
        DistilBertTokenizer tokenizer = DistilBertTokenizer.builder(
            Arrays.asList("Elastic", "##search", "fun", BertTokenizer.CLASS_TOKEN, BertTokenizer.SEPARATOR_TOKEN),
            new DistilBertTokenizationParams(null, null, 512)
        ).build();

        DistilBertRequestBuilder requestBuilder = new DistilBertRequestBuilder(tokenizer);
        BytesReference bytesReference = requestBuilder.buildRequest("Elasticsearch fun", "request1").processInput;

        Map<String, Object> jsonDocAsMap = XContentHelper.convertToMap(bytesReference, true, XContentType.JSON).v2();

        assertThat(jsonDocAsMap.keySet(), hasSize(3));
        assertEquals("request1", jsonDocAsMap.get("request_id"));
        assertEquals(Arrays.asList(3, 0, 1, 2, 4), jsonDocAsMap.get("tokens"));
        assertEquals(Arrays.asList(1, 1, 1, 1, 1), jsonDocAsMap.get("arg_1"));
    }

    public void testInputTooLarge() throws IOException {
        DistilBertTokenizer tokenizer = DistilBertTokenizer.builder(
            Arrays.asList("Elastic", "##search", "fun", BertTokenizer.CLASS_TOKEN, BertTokenizer.SEPARATOR_TOKEN),
            new DistilBertTokenizationParams(null, null, 5)
        ).build();
        {
            DistilBertRequestBuilder requestBuilder = new DistilBertRequestBuilder(tokenizer);
            ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
                () -> requestBuilder.buildRequest("Elasticsearch fun Elasticsearch fun Elasticsearch fun", "request1"));

            assertThat(e.getMessage(),
                containsString("Input too large. The tokenized input length [11] exceeds the maximum sequence length [5]"));
        }
        {
            DistilBertRequestBuilder requestBuilder = new DistilBertRequestBuilder(tokenizer);
            // input will become 3 tokens + the Class and Separator token = 5 which is
            // our max sequence length
            requestBuilder.buildRequest("Elasticsearch fun", "request1");
        }
    }
}
