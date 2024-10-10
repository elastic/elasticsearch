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
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class RetrieverProfileResult implements ToXContentObject, Writeable {

    private final String name;
    private long tookInMillis;
    private final Map<String, Long> breakdownMap;
    private final List<RetrieverProfileResult> children;

    public RetrieverProfileResult(String name, long tookInMillis, Map<String, Long> breakdownMap, List<RetrieverProfileResult> children) {
        this.name = name;
        this.tookInMillis = tookInMillis;
        this.breakdownMap = breakdownMap;
        this.children = children;
    }

    public RetrieverProfileResult(StreamInput in) throws IOException {
        name = in.readString();
        tookInMillis = in.readLong();
        breakdownMap = in.readMap(StreamInput::readString, StreamInput::readLong);
        children = in.readCollectionAsList(RetrieverProfileResult::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeLong(tookInMillis);
        out.writeMap(breakdownMap, StreamOutput::writeString, StreamOutput::writeLong);
        out.writeCollection(children);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(name);
        if (tookInMillis != -1) {
            builder.field("took_in_millis", tookInMillis);
        }
        if (false == breakdownMap.isEmpty()) {
            builder.field("breakdown", breakdownMap);
        }
        if (false == children.isEmpty()) {
            builder.startArray("children");
            for (RetrieverProfileResult child : children) {
                child.toXContent(builder, params);
            }
            builder.endArray();
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    public List<RetrieverProfileResult> getChildren() {
        return children;
    }

    public void addChild(RetrieverProfileResult profileResult) {
        assert profileResult != null;
        children.add(profileResult);
    }

    public Map<String, Long> getBreakdownMap() {
        return breakdownMap;
    }

    public void setTookInMillis(long tookInMillis) {
        this.tookInMillis = tookInMillis;
    }
}
