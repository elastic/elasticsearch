/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.action.util.QueryPage;

import java.io.IOException;
import java.util.Objects;

public abstract class AbstractGetResourcesResponse<T extends ToXContent & Writeable> extends ActionResponse implements ToXContentObject {

    private QueryPage<T> resources;

    protected AbstractGetResourcesResponse() {}

    @SuppressWarnings("this-escape")
    protected AbstractGetResourcesResponse(StreamInput in) throws IOException {
        super(in);
        resources = new QueryPage<>(in, getReader());
    }

    protected AbstractGetResourcesResponse(QueryPage<T> resources) {
        this.resources = Objects.requireNonNull(resources);
    }

    public QueryPage<T> getResources() {
        return resources;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        resources.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        resources.doXContentBody(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(resources);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj instanceof AbstractGetResourcesResponse == false) {
            return false;
        }
        AbstractGetResourcesResponse<?> other = (AbstractGetResourcesResponse<?>) obj;
        return Objects.equals(resources, other.resources);
    }

    @Override
    public final String toString() {
        return Strings.toString(this);
    }

    protected abstract Reader<T> getReader();
}
