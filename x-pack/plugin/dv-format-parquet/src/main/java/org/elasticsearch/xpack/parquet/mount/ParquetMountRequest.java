/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.mount;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

/**
 * A request to preview or to mount Parquet files. It carries the body as it came in, because the node that runs the action parses it
 * with {@link ParquetMountSpec#parse} and reports a body that is not valid as one error of the request.
 */
public final class ParquetMountRequest extends ActionRequest {

    private final Map<String, Object> body;

    public ParquetMountRequest(Map<String, Object> body) {
        this.body = body;
    }

    public ParquetMountRequest(StreamInput in) throws IOException {
        super(in);
        this.body = in.readGenericMap();
    }

    public static ParquetMountRequest fromXContent(XContentParser parser) throws IOException {
        return new ParquetMountRequest(parser.map());
    }

    public Map<String, Object> body() {
        return body;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeGenericMap(body);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
