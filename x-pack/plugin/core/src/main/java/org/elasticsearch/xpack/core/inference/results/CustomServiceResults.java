/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.core.ml.inference.results.CustomResults;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public class CustomServiceResults implements InferenceServiceResults {
    public static final String NAME = "custom_service_results";
    public static final String CUSTOM_TYPE = TaskType.CUSTOM.name().toLowerCase(Locale.ROOT);

    Map<String, Object> data;

    public CustomServiceResults(Map<String, Object> data) {
        this.data = data;
    }

    public CustomServiceResults(StreamInput in) throws IOException {
        this.data = in.readGenericMap();
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return ChunkedToXContentHelper.object(CUSTOM_TYPE, this.asMap());
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericMap(data);
    }

    @Override
    public List<? extends InferenceResults> transformToCoordinationFormat() {
        return transformToLegacyFormat();
    }

    @Override
    public List<? extends InferenceResults> transformToLegacyFormat() {
        return List.of(new CustomResults(data));
    }

    @Override
    public Map<String, Object> asMap() {
        return data;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(NAME);
        sb.append(Integer.toHexString(hashCode()));
        sb.append("\n");
        sb.append(this.asMap().toString());
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CustomServiceResults that = (CustomServiceResults) o;
        return data.equals(that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data);
    }
}
