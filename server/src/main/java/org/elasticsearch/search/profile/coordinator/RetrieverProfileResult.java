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
import java.util.Map;

public class RetrieverProfileResult implements ToXContentObject, Writeable {

    private final String name;
    private long tookInMillis;
    private final Map<String, SearchProfileCoordinatorResult> children;

    public RetrieverProfileResult(String name, long tookInMillis, Map<String, SearchProfileCoordinatorResult> children) {
        this.name = name;
        this.tookInMillis = tookInMillis;
        this.children = children;
    }

    public RetrieverProfileResult(StreamInput in) throws IOException {
        name = in.readString();
        tookInMillis = in.readLong();
        children = in.readMap(StreamInput::readString, SearchProfileCoordinatorResult::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeLong(tookInMillis);
        out.writeMap(children, StreamOutput::writeString, StreamOutput::writeWriteable);
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
            for (Map.Entry<String, SearchProfileCoordinatorResult> child : children.entrySet()) {
                builder.startObject();
                builder.startObject(child.getKey());
                child.getValue().toXContent(builder, params);
                builder.endObject();
                builder.endObject();
            }
            builder.endArray();
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    public void addChild(String name, SearchProfileCoordinatorResult profileResult) {
        assert profileResult != null;
        children.put(name, profileResult);
    }

    public void setTookInMillis(long tookInMillis) {
        this.tookInMillis = tookInMillis;
    }
}
