/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pipelines.nlp;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class NerProcessorTests extends ESTestCase {

    public void testJsonRequest() throws IOException {
        String requestId = "foo";
        int [] tokens = new int[] {100, 101, 102, 103, 104};
        BytesReference bytesReference = NerProcessor.jsonRequest(tokens, requestId);

        String jsonDoc = bytesReference.utf8ToString();
        assertEquals(
            "{\"request_id\":\"foo\",\"tokens\":[100,101,102,103,104],\"arg_1\":[1,1,1,1,1],\"arg_2\":[0,0,0,0,0],\"arg_3\":[0,1,2,3,4]}",
            jsonDoc);
    }
}
