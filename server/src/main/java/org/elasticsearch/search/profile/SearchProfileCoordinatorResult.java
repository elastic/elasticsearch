/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.profile;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.profile.coordinator.RetrieverProfileResult;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Profile results from a particular shard for all search phases.
 */
public class SearchProfileCoordinatorResult implements Writeable, ToXContentFragment {

    private final String nodeId;
    private long tookInMillis;
    private final RetrieverProfileResult retrieverProfileResult;
    private final Map<String, Long> breakdownMap;

    public SearchProfileCoordinatorResult(String nodeId, RetrieverProfileResult retrieverProfileResult, Map<String, Long> breakdownMap) {
        this.nodeId = nodeId;
        this.retrieverProfileResult = retrieverProfileResult;
        this.breakdownMap = breakdownMap;
    }

    public SearchProfileCoordinatorResult(StreamInput in) throws IOException {
        nodeId = in.readString();
        tookInMillis = in.readLong();
        retrieverProfileResult = in.readOptionalWriteable(RetrieverProfileResult::new);
        breakdownMap = in.readMap(StreamInput::readString, StreamInput::readLong);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(nodeId);
        out.writeLong(tookInMillis);
        out.writeOptionalWriteable(retrieverProfileResult);
        out.writeMap(breakdownMap, StreamOutput::writeString, StreamOutput::writeLong);
    }

    public String getNodeId() {
        return this.nodeId;
    }

    public void setTookInMillis(long tookInMillis) {
        this.tookInMillis = tookInMillis;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("node_id", nodeId);
        if (tookInMillis != -1) {
            builder.field("took_in_millis", tookInMillis);
        }
        if (retrieverProfileResult != null) {
            builder.field("retriever", retrieverProfileResult);
        }
        if (false == breakdownMap.isEmpty()) {
            builder.field("breakdown", breakdownMap);
        }
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchProfileCoordinatorResult that = (SearchProfileCoordinatorResult) o;
        return nodeId.equals(that.nodeId)
            && tookInMillis == that.tookInMillis
            && Objects.equals(retrieverProfileResult, that.retrieverProfileResult)
            && Objects.equals(breakdownMap, that.breakdownMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, tookInMillis, retrieverProfileResult, breakdownMap);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
