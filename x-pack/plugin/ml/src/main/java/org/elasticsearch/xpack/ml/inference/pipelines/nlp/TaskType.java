/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pipelines.nlp;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;

public enum TaskType {

    TOKEN_CLASSIFICATION {
        public BytesReference jsonRequest(int[] tokens, String requestId) throws IOException {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder.field(REQUEST_ID, requestId);
            builder.array(TOKENS, tokens);

            int[] inputMask = new int[tokens.length];
            Arrays.fill(inputMask, 1);
            int[] segmentMask = new int[tokens.length];
            Arrays.fill(segmentMask, 0);
            int[] positionalIds = new int[tokens.length];
            Arrays.setAll(positionalIds, i -> i);
            builder.array(ARG1, inputMask);
            builder.array(ARG2, segmentMask);
            builder.array(ARG3, positionalIds);
            builder.endObject();

            // BytesReference.bytes closes the builder
            return BytesReference.bytes(builder);
        }
    };

    public BytesReference jsonRequest(int[] tokens, String requestId) throws IOException {
        throw new UnsupportedOperationException("json request must be specialised for task type [" + this.name() + "]");
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static TaskType fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    private static final String REQUEST_ID = "request_id";
    private static final String TOKENS = "tokens";
    private static final String ARG1 = "arg_1";
    private static final String ARG2 = "arg_2";
    private static final String ARG3 = "arg_3";
}
