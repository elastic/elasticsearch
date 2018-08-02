/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.featureindexbuilder.job;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * This class holds the configuration details of a feature index builder job
 */
public class FeatureIndexBuilderJobConfig implements NamedWriteable, ToXContentObject {
    private static final String NAME = "xpack/feature_index_builder/jobconfig";

    public static final ParseField ID = new ParseField("id");

    private String id;

    public static final ObjectParser<FeatureIndexBuilderJobConfig.Builder, Void> PARSER = new ObjectParser<>(NAME, false,
            FeatureIndexBuilderJobConfig.Builder::new);

    static {
        PARSER.declareString(FeatureIndexBuilderJobConfig.Builder::setId, ID);
    }

    FeatureIndexBuilderJobConfig(String id) {
        this.id = id;
    }

    public FeatureIndexBuilderJobConfig(StreamInput in) throws IOException {
        id = in.readString();
    }

    public FeatureIndexBuilderJobConfig() {
    }

    public String getId() {
        return id;
    }

    public String getCron() {
        return "*";
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (id != null) {
            // to be replace by constant
            builder.field("id", id);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        FeatureIndexBuilderJobConfig that = (FeatureIndexBuilderJobConfig) other;

        return Objects.equals(this.id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public static class Builder implements Writeable, ToXContentObject {
        private String id;
        
        public Builder() {}
        
        public Builder(FeatureIndexBuilderJobConfig job) {
            this.setId(job.getId());
        }
        
        public static FeatureIndexBuilderJobConfig.Builder fromXContent(String id, XContentParser parser) {
            FeatureIndexBuilderJobConfig.Builder config = FeatureIndexBuilderJobConfig.PARSER.apply(parser, null);
            if (id != null) {
                config.setId(id);
            }
            return config;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (id != null) {
                builder.field(ID.getPreferredName(), id);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(id);
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }
        
        public FeatureIndexBuilderJobConfig build() {
            return new FeatureIndexBuilderJobConfig(id);
        }
    }
}
