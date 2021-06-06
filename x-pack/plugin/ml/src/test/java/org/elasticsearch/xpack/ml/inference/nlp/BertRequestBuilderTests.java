/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static org.hamcrest.Matchers.hasSize;

public class BertRequestBuilderTests extends ESTestCase {

    public void testBuildRequest() throws IOException {
        BertTokenizer tokenizer = BertTokenizer.builder(
            Arrays.asList("Elastic", "##search", "fun", BertTokenizer.CLASS_TOKEN, BertTokenizer.SEPARATOR_TOKEN)).build();

        BertRequestBuilder requestBuilder = new BertRequestBuilder(tokenizer);
        BytesReference bytesReference = requestBuilder.buildRequest("Elasticsearch fun", "request1");

        Map<String, Object> jsonDocAsMap = XContentHelper.convertToMap(bytesReference, true, XContentType.JSON).v2();

        assertThat(jsonDocAsMap.keySet(), hasSize(5));
        assertEquals("request1", jsonDocAsMap.get("request_id"));
        assertEquals(Arrays.asList(3, 0, 1, 2, 4), jsonDocAsMap.get("tokens"));
        assertEquals(Arrays.asList(1, 1, 1, 1, 1), jsonDocAsMap.get("arg_1"));
        assertEquals(Arrays.asList(0, 0, 0, 0, 0), jsonDocAsMap.get("arg_2"));
        assertEquals(Arrays.asList(0, 1, 2, 3, 4), jsonDocAsMap.get("arg_3"));
    }
}
