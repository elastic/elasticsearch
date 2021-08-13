/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.dataframe;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ObjectParser.ValueType.VALUE;

public class DataFrameAnalyticsConfigUpdate implements ToXContentObject {

    public static DataFrameAnalyticsConfigUpdate fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null).build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>("data_frame_analytics_config_update", true, Builder::new);

    static {
        PARSER.declareString(Builder::setId, DataFrameAnalyticsConfig.ID);
        PARSER.declareStringOrNull(Builder::setDescription, DataFrameAnalyticsConfig.DESCRIPTION);
        PARSER.declareField(
            Builder::setModelMemoryLimit,
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), DataFrameAnalyticsConfig.MODEL_MEMORY_LIMIT.getPreferredName()),
            DataFrameAnalyticsConfig.MODEL_MEMORY_LIMIT,
            VALUE);
        PARSER.declareBoolean(Builder::setAllowLazyStart, DataFrameAnalyticsConfig.ALLOW_LAZY_START);
        PARSER.declareInt(Builder::setMaxNumThreads, DataFrameAnalyticsConfig.MAX_NUM_THREADS);
    }

    private final String id;
    private final String description;
    private final ByteSizeValue modelMemoryLimit;
    private final Boolean allowLazyStart;
    private final Integer maxNumThreads;

    private DataFrameAnalyticsConfigUpdate(String id,
                                           @Nullable String description,
                                           @Nullable ByteSizeValue modelMemoryLimit,
                                           @Nullable Boolean allowLazyStart,
                                           @Nullable Integer maxNumThreads) {
        this.id = id;
        this.description = description;
        this.modelMemoryLimit = modelMemoryLimit;
        this.allowLazyStart = allowLazyStart;
        this.maxNumThreads = maxNumThreads;
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

        private Builder() {}

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
