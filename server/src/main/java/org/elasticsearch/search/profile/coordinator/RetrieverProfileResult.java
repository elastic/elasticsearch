/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.profile.coordinator;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.profile.SearchProfileCoordinatorResult;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public class RetrieverProfileResult implements ToXContentObject, Writeable {

    private final String name;
    private long tookInMillis;
    private final List<SearchProfileCoordinatorResult> children;

    public RetrieverProfileResult(String name, long tookInMillis, List<SearchProfileCoordinatorResult> children) {
        this.name = name;
        this.tookInMillis = tookInMillis;
        this.children = children;
    }

    public RetrieverProfileResult(StreamInput in) throws IOException {
        name = in.readString();
        tookInMillis = in.readLong();
        children = in.readCollectionAsList(SearchProfileCoordinatorResult::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeLong(tookInMillis);
        out.writeCollection(children);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(name);
        if (tookInMillis != -1) {
            builder.field("took_in_millis", tookInMillis);
        }
        if (false == children.isEmpty()) {
            builder.startArray("children");
            for (SearchProfileCoordinatorResult child : children) {
                builder.startObject();
                child.toXContent(builder, params);
                builder.endObject();
            }
            builder.endArray();
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    public List<SearchProfileCoordinatorResult> getChildren() {
        return children;
    }

    public void addChild(SearchProfileCoordinatorResult profileResult) {
        assert profileResult != null;
        children.add(profileResult);
    }

    public void setTookInMillis(long tookInMillis) {
        this.tookInMillis = tookInMillis;
    }
}
