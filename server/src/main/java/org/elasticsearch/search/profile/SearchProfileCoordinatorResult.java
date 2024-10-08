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
import java.util.Objects;

/**
 * Profile results from a particular shard for all search phases.
 */
public class SearchProfileCoordinatorResult implements Writeable, ToXContentFragment {

    private final String nodeId;
    private final RetrieverProfileResult retrieverProfileResult;

    public SearchProfileCoordinatorResult(String nodeId, RetrieverProfileResult retrieverProfileResult) {
        this.nodeId = nodeId;
        this.retrieverProfileResult = retrieverProfileResult;
    }

    public SearchProfileCoordinatorResult(StreamInput in) throws IOException {
        nodeId = in.readString();
        retrieverProfileResult = in.readOptionalWriteable(RetrieverProfileResult::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(nodeId);
        out.writeOptionalWriteable(retrieverProfileResult);
    }

    public String getNodeId() {
        return this.nodeId;
    }

    public RetrieverProfileResult getRetrieverProfileResult() {
        return retrieverProfileResult;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (retrieverProfileResult != null) {
            builder.field("retriever", retrieverProfileResult);
        }
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchProfileCoordinatorResult that = (SearchProfileCoordinatorResult) o;
        return Objects.equals(retrieverProfileResult, that.retrieverProfileResult);
    }

    @Override
    public int hashCode() {
        return Objects.hash(retrieverProfileResult);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
