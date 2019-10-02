/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml.inference;

import org.elasticsearch.Version;
import org.elasticsearch.client.common.TimeUtil;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class TrainedModelConfig implements ToXContentObject {

    public static final String NAME = "trained_model_doc";

    public static final ParseField MODEL_ID = new ParseField("model_id");
    public static final ParseField CREATED_BY = new ParseField("created_by");
    public static final ParseField VERSION = new ParseField("version");
    public static final ParseField DESCRIPTION = new ParseField("description");
    public static final ParseField CREATED_TIME = new ParseField("created_time");
    public static final ParseField MODEL_VERSION = new ParseField("model_version");
    public static final ParseField DEFINITION = new ParseField("definition");
    public static final ParseField MODEL_TYPE = new ParseField("model_type");
    public static final ParseField METADATA = new ParseField("metadata");

    public static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>(NAME,
            true,
            TrainedModelConfig.Builder::new);
    static {
        PARSER.declareString(TrainedModelConfig.Builder::setModelId, MODEL_ID);
        PARSER.declareString(TrainedModelConfig.Builder::setCreatedBy, CREATED_BY);
        PARSER.declareString(TrainedModelConfig.Builder::setVersion, VERSION);
        PARSER.declareString(TrainedModelConfig.Builder::setDescription, DESCRIPTION);
        PARSER.declareField(TrainedModelConfig.Builder::setCreatedTime,
            (p, c) -> TimeUtil.parseTimeFieldToInstant(p, CREATED_TIME.getPreferredName()),
            CREATED_TIME,
            ObjectParser.ValueType.VALUE);
        PARSER.declareLong(TrainedModelConfig.Builder::setModelVersion, MODEL_VERSION);
        PARSER.declareString(TrainedModelConfig.Builder::setModelType, MODEL_TYPE);
        PARSER.declareObject(TrainedModelConfig.Builder::setMetadata, (p, c) -> p.map(), METADATA);
        PARSER.declareObject(TrainedModelConfig.Builder::setDefinition,
            (p, c) -> TrainedModelDefinition.fromXContent(p),
            DEFINITION);
    }

    public static TrainedModelConfig.Builder fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final String modelId;
    private final String createdBy;
    private final Version version;
    private final String description;
    private final Instant createdTime;
    private final Long modelVersion;
    private final String modelType;
    private final Map<String, Object> metadata;
    private final TrainedModelDefinition definition;

    TrainedModelConfig(String modelId,
                       String createdBy,
                       Version version,
                       String description,
                       Instant createdTime,
                       Long modelVersion,
                       String modelType,
                       TrainedModelDefinition definition,
                       Map<String, Object> metadata) {
        this.modelId = modelId;
        this.createdBy = createdBy;
        this.version = version;
        this.createdTime = Instant.ofEpochMilli(createdTime.toEpochMilli());
        this.modelType = modelType;
        this.definition = definition;
        this.description = description;
        this.metadata = metadata == null ? null : Collections.unmodifiableMap(metadata);
        this.modelVersion = modelVersion;
    }

    public String getModelId() {
        return modelId;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public Version getVersion() {
        return version;
    }

    public String getDescription() {
        return description;
    }

    public Instant getCreatedTime() {
        return createdTime;
    }

    public Long getModelVersion() {
        return modelVersion;
    }

    public String getModelType() {
        return modelType;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public TrainedModelDefinition getDefinition() {
        return definition;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (modelId != null) {
            builder.field(MODEL_ID.getPreferredName(), modelId);
        }
        if (createdBy != null) {
            builder.field(CREATED_BY.getPreferredName(), createdBy);
        }
        if (version != null) {
            builder.field(VERSION.getPreferredName(), version.toString());
        }
        if (description != null) {
            builder.field(DESCRIPTION.getPreferredName(), description);
        }
        if (createdTime != null) {
            builder.timeField(CREATED_TIME.getPreferredName(), CREATED_TIME.getPreferredName() + "_string", createdTime.toEpochMilli());
        }
        if (modelVersion != null) {
            builder.field(MODEL_VERSION.getPreferredName(), modelVersion);
        }
        if (modelType != null) {
            builder.field(MODEL_TYPE.getPreferredName(), modelType);
        }
        if (definition != null) {
            builder.field(DEFINITION.getPreferredName(), definition);
        }
        if (metadata != null) {
            builder.field(METADATA.getPreferredName(), metadata);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TrainedModelConfig that = (TrainedModelConfig) o;
        return Objects.equals(modelId, that.modelId) &&
            Objects.equals(createdBy, that.createdBy) &&
            Objects.equals(version, that.version) &&
            Objects.equals(description, that.description) &&
            Objects.equals(createdTime, that.createdTime) &&
            Objects.equals(modelVersion, that.modelVersion) &&
            Objects.equals(modelType, that.modelType) &&
            Objects.equals(definition, that.definition) &&
            Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId,
            createdBy,
            version,
            createdTime,
            modelType,
            definition,
            description,
            metadata,
            modelVersion);
    }


    public static class Builder {

        private String modelId;
        private String createdBy;
        private Version version;
        private String description;
        private Instant createdTime;
        private Long modelVersion;
        private String modelType;
        private Map<String, Object> metadata;
        private TrainedModelDefinition.Builder definition;

        public Builder setModelId(String modelId) {
            this.modelId = modelId;
            return this;
        }

        private Builder setCreatedBy(String createdBy) {
            this.createdBy = createdBy;
            return this;
        }

        private Builder setVersion(Version version) {
            this.version = version;
            return this;
        }

        private Builder setVersion(String version) {
            return this.setVersion(Version.fromString(version));
        }

        public Builder setDescription(String description) {
            this.description = description;
            return this;
        }

        private Builder setCreatedTime(Instant createdTime) {
            this.createdTime = createdTime;
            return this;
        }

        public Builder setModelVersion(Long modelVersion) {
            this.modelVersion = modelVersion;
            return this;
        }

        public Builder setModelType(String modelType) {
            this.modelType = modelType;
            return this;
        }

        public Builder setMetadata(Map<String, Object> metadata) {
            this.metadata = metadata;
            return this;
        }

        public Builder setDefinition(TrainedModelDefinition.Builder definition) {
            this.definition = definition;
            return this;
        }

        public TrainedModelConfig build() {
            return new TrainedModelConfig(
                modelId,
                createdBy,
                version,
                description,
                createdTime,
                modelVersion,
                modelType,
                definition == null ? null : definition.build(),
                metadata);
        }
    }
}
