/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.transport;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.sql.action.compute.data.Page;

import java.io.IOException;
import java.util.List;

public class ComputeResponse extends ActionResponse implements ToXContentObject {
    private final List<Page> pages;

    public ComputeResponse(StreamInput in) {
        throw new UnsupportedOperationException();
    }

    public ComputeResponse(List<Page> pages) {
        super();
        this.pages = pages;
    }

    public List<Page> getPages() {
        return pages;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("pages", pages.size());
        builder.field("rows", pages.stream().mapToInt(Page::getPositionCount).sum());
        builder.endObject();
        return builder;
    }
}
