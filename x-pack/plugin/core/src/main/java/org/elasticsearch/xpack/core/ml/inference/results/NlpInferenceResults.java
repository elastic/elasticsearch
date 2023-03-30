/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

abstract class NlpInferenceResults implements InferenceResults {

    protected final boolean isTruncated;

    NlpInferenceResults(boolean isTruncated) {
        this.isTruncated = isTruncated;
    }

    NlpInferenceResults(StreamInput in) throws IOException {
        this.isTruncated = in.readBoolean();
    }

    abstract void doXContentBody(XContentBuilder builder, Params params) throws IOException;

    abstract void doWriteTo(StreamOutput out) throws IOException;

    abstract void addMapFields(Map<String, Object> map);

    public boolean isTruncated() {
        return isTruncated;
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(isTruncated);
        doWriteTo(out);
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        doXContentBody(builder, params);
        if (isTruncated) {
            builder.field("is_truncated", isTruncated);
        }
        return builder;
    }

    @Override
    public final Map<String, Object> asMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        addMapFields(map);
        if (isTruncated) {
            map.put("is_truncated", isTruncated);
        }
        return map;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NlpInferenceResults that = (NlpInferenceResults) o;
        return isTruncated == that.isTruncated;
    }

    @Override
    public int hashCode() {
        return Objects.hash(isTruncated);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
