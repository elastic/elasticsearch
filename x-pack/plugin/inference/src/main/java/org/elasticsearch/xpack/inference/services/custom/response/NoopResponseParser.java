/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom.response;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.io.IOException;

public record NoopResponseParser() implements CustomResponseParser {

    public static final String NAME = "noop_response_parser";
    public static final NoopResponseParser INSTANCE = new NoopResponseParser();

    public static NoopResponseParser fromMap() {
        return new NoopResponseParser();
    }

    public NoopResponseParser(StreamInput in) {
        this();
    }

    public void writeTo(StreamOutput out) throws IOException {}

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public InferenceServiceResults parse(HttpResult result) {
        return null;
    }
}
