/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class EsqlQueryResponse extends ActionResponse implements ToXContentObject {

    public EsqlQueryResponse(StreamInput in) throws IOException {
        super(in);
    }

    public EsqlQueryResponse() {
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray("columns");
        builder.endArray();
        builder.startArray("values");
        builder.endArray();
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
    }
}
