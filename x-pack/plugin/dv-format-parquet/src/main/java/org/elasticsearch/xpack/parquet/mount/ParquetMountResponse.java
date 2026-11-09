/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.mount;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

/** The answer of a preview or of a mount: an object that the action built, with nothing in it that needs a type of its own. */
public final class ParquetMountResponse extends ActionResponse implements ToXContentObject {

    private final Map<String, Object> body;

    public ParquetMountResponse(Map<String, Object> body) {
        this.body = body;
    }

    public ParquetMountResponse(StreamInput in) throws IOException {
        this.body = in.readGenericMap();
    }

    public Map<String, Object> body() {
        return body;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericMap(body);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (Map.Entry<String, Object> entry : body.entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        return builder.endObject();
    }
}
