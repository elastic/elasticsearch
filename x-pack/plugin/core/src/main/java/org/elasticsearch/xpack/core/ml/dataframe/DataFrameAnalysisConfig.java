/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class DataFrameAnalysisConfig implements ToXContentObject, Writeable {

    public static ContextParser<Void, DataFrameAnalysisConfig> parser() {
        return (p, c) -> new DataFrameAnalysisConfig(p.mapOrdered());
    }

    private final Map<String, Object> config;

    public DataFrameAnalysisConfig(Map<String, Object> config) {
        this.config = Objects.requireNonNull(config);
        if (config.size() != 1) {
            throw ExceptionsHelper.badRequestException("A data frame analysis must specify exactly one analysis type");
        }
    }

    public DataFrameAnalysisConfig(StreamInput in) throws IOException {
        config = in.readMap();
    }

    public Map<String, Object> asMap() {
        return config;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(config);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.map(config);
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataFrameAnalysisConfig that = (DataFrameAnalysisConfig) o;
        return Objects.equals(config, that.config);
    }

    @Override
    public int hashCode() {
        return Objects.hash(config);
    }
}
