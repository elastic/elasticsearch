/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.xcontent.ObjectParser.ValueType.VALUE;

public class DataFrameAnalyticsConfigUpdate implements Writeable, ToXContentObject {

    public static final ConstructingObjectParser<Builder, Void> PARSER = new ConstructingObjectParser<>(
        "data_frame_analytics_config_update",
        args -> new Builder((String) args[0])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), DataFrameAnalyticsConfig.ID);
        PARSER.declareStringOrNull(Builder::setDescription, DataFrameAnalyticsConfig.DESCRIPTION);
        PARSER.declareField(
            Builder::setModelMemoryLimit,
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), DataFrameAnalyticsConfig.MODEL_MEMORY_LIMIT.getPreferredName()),
            DataFrameAnalyticsConfig.MODEL_MEMORY_LIMIT,
            VALUE
        );
        PARSER.declareBoolean(Builder::setAllowLazyStart, DataFrameAnalyticsConfig.ALLOW_LAZY_START);
        PARSER.declareInt(Builder::setMaxNumThreads, DataFrameAnalyticsConfig.MAX_NUM_THREADS);
    }

    private final String id;
    private final String description;
    private final ByteSizeValue modelMemoryLimit;
    private final Boolean allowLazyStart;
    private final Integer maxNumThreads;

    private DataFrameAnalyticsConfigUpdate(
        String id,
        @Nullable String description,
        @Nullable ByteSizeValue modelMemoryLimit,
        @Nullable Boolean allowLazyStart,
        @Nullable Integer maxNumThreads
    ) {
        this.id = id;
        this.description = description;
        this.modelMemoryLimit = modelMemoryLimit;
        this.allowLazyStart = allowLazyStart;

        if (maxNumThreads != null && maxNumThreads < 1) {
            throw ExceptionsHelper.badRequestException(
                "[{}] must be a positive integer",
                DataFrameAnalyticsConfig.MAX_NUM_THREADS.getPreferredName()
            );
        }
        this.maxNumThreads = maxNumThreads;
    }

    public DataFrameAnalyticsConfigUpdate(StreamInput in) throws IOException {
        this.id = in.readString();
        this.description = in.readOptionalString();
        this.modelMemoryLimit = in.readOptionalWriteable(ByteSizeValue::new);
        this.allowLazyStart = in.readOptionalBoolean();
        this.maxNumThreads = in.readOptionalVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeOptionalString(description);
        out.writeOptionalWriteable(modelMemoryLimit);
        out.writeOptionalBoolean(allowLazyStart);
        out.writeOptionalVInt(maxNumThreads);
    }

    public String getId() {
        return id;
    }

    public String getDescription() {
        return description;
    }

    public ByteSizeValue getModelMemoryLimit() {
        return modelMemoryLimit;
    }

    public Boolean isAllowLazyStart() {
        return allowLazyStart;
    }

    public Integer getMaxNumThreads() {
        return maxNumThreads;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DataFrameAnalyticsConfig.ID.getPreferredName(), id);
        if (description != null) {
            builder.field(DataFrameAnalyticsConfig.DESCRIPTION.getPreferredName(), description);
        }
        if (modelMemoryLimit != null) {
            builder.field(DataFrameAnalyticsConfig.MODEL_MEMORY_LIMIT.getPreferredName(), modelMemoryLimit.getStringRep());
        }
        if (allowLazyStart != null) {
            builder.field(DataFrameAnalyticsConfig.ALLOW_LAZY_START.getPreferredName(), allowLazyStart);
        }
        if (maxNumThreads != null) {
            builder.field(DataFrameAnalyticsConfig.MAX_NUM_THREADS.getPreferredName(), maxNumThreads);
        }
        builder.endObject();
        return builder;
    }

    /**
     * Updates {@code source} with the new values in this object returning a new {@link DataFrameAnalyticsConfig}.
     *
     * @param source Source config to be updated
     * @return A new config equivalent to {@code source} updated.
     */
    public DataFrameAnalyticsConfig.Builder mergeWithConfig(DataFrameAnalyticsConfig source) {
        if (id.equals(source.getId()) == false) {
            throw new IllegalArgumentException("Cannot apply update to a config with different id");
        }

        DataFrameAnalyticsConfig.Builder builder = new DataFrameAnalyticsConfig.Builder(source);
        if (description != null) {
            builder.setDescription(description);
        }
        if (modelMemoryLimit != null) {
            builder.setModelMemoryLimit(modelMemoryLimit);
        }
        if (allowLazyStart != null) {
            builder.setAllowLazyStart(allowLazyStart);
        }
        if (maxNumThreads != null) {
            builder.setMaxNumThreads(maxNumThreads);
        }
        return builder;
    }

    /**
     * Whether this update applied to the given source config requires analytics task restart.
     */
    public boolean requiresRestart(DataFrameAnalyticsConfig source) {
        return (getModelMemoryLimit() != null && getModelMemoryLimit().equals(source.getModelMemoryLimit()) == false)
            || (getMaxNumThreads() != null && getMaxNumThreads().equals(source.getMaxNumThreads()) == false);
    }

    public Set<String> getUpdatedFields() {
        Set<String> updatedFields = new TreeSet<>();
        if (description != null) {
            updatedFields.add(DataFrameAnalyticsConfig.DESCRIPTION.getPreferredName());
        }
        if (modelMemoryLimit != null) {
            updatedFields.add(DataFrameAnalyticsConfig.MODEL_MEMORY_LIMIT.getPreferredName());
        }
        if (allowLazyStart != null) {
            updatedFields.add(DataFrameAnalyticsConfig.ALLOW_LAZY_START.getPreferredName());
        }
        if (maxNumThreads != null) {
            updatedFields.add(DataFrameAnalyticsConfig.MAX_NUM_THREADS.getPreferredName());
        }
        return updatedFields;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other instanceof DataFrameAnalyticsConfigUpdate == false) {
            return false;
        }

        DataFrameAnalyticsConfigUpdate that = (DataFrameAnalyticsConfigUpdate) other;

        return Objects.equals(this.id, that.id)
            && Objects.equals(this.description, that.description)
            && Objects.equals(this.modelMemoryLimit, that.modelMemoryLimit)
            && Objects.equals(this.allowLazyStart, that.allowLazyStart)
            && Objects.equals(this.maxNumThreads, that.maxNumThreads);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, description, modelMemoryLimit, allowLazyStart, maxNumThreads);
    }

    public static class Builder {

        private String id;
        private String description;
        private ByteSizeValue modelMemoryLimit;
        private Boolean allowLazyStart;
        private Integer maxNumThreads;

        public Builder(String id) {
            this.id = id;
        }

        public String getId() {
            return id;
        }

        public Builder setId(String id) {
            this.id = id;
            return this;
        }

        public Builder setDescription(String description) {
            this.description = description;
            return this;
        }

        public Builder setModelMemoryLimit(ByteSizeValue modelMemoryLimit) {
            this.modelMemoryLimit = modelMemoryLimit;
            return this;
        }

        public Builder setAllowLazyStart(Boolean allowLazyStart) {
            this.allowLazyStart = allowLazyStart;
            return this;
        }

        public Builder setMaxNumThreads(Integer maxNumThreads) {
            this.maxNumThreads = maxNumThreads;
            return this;
        }

        public DataFrameAnalyticsConfigUpdate build() {
            return new DataFrameAnalyticsConfigUpdate(id, description, modelMemoryLimit, allowLazyStart, maxNumThreads);
        }
    }
}
