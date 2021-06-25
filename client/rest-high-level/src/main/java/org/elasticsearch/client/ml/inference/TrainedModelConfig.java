/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.inference;

import org.elasticsearch.Version;
import org.elasticsearch.client.common.TimeUtil;
import org.elasticsearch.client.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.client.ml.inference.trainedmodel.TrainedModelLocation;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.client.ml.inference.NamedXContentObjectHelper.writeNamedObject;

public class TrainedModelConfig implements ToXContentObject {

    public static final String NAME = "trained_model_config";

    public static final ParseField MODEL_ID = new ParseField("model_id");
    public static final ParseField MODEL_TYPE = new ParseField("model_type");
    public static final ParseField CREATED_BY = new ParseField("created_by");
    public static final ParseField VERSION = new ParseField("version");
    public static final ParseField DESCRIPTION = new ParseField("description");
    public static final ParseField CREATE_TIME = new ParseField("create_time");
    public static final ParseField DEFINITION = new ParseField("definition");
    public static final ParseField COMPRESSED_DEFINITION = new ParseField("compressed_definition");
    public static final ParseField TAGS = new ParseField("tags");
    public static final ParseField METADATA = new ParseField("metadata");
    public static final ParseField INPUT = new ParseField("input");
    public static final ParseField ESTIMATED_HEAP_MEMORY_USAGE_BYTES = new ParseField("estimated_heap_memory_usage_bytes");
    public static final ParseField ESTIMATED_OPERATIONS = new ParseField("estimated_operations");
    public static final ParseField LICENSE_LEVEL = new ParseField("license_level");
    public static final ParseField DEFAULT_FIELD_MAP = new ParseField("default_field_map");
    public static final ParseField INFERENCE_CONFIG = new ParseField("inference_config");
    public static final ParseField LOCATION = new ParseField("location");

    public static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>(NAME,
            true,
            TrainedModelConfig.Builder::new);
    static {
        PARSER.declareString(TrainedModelConfig.Builder::setModelId, MODEL_ID);
        PARSER.declareString(TrainedModelConfig.Builder::setModelType, MODEL_TYPE);
        PARSER.declareString(TrainedModelConfig.Builder::setCreatedBy, CREATED_BY);
        PARSER.declareString(TrainedModelConfig.Builder::setVersion, VERSION);
        PARSER.declareString(TrainedModelConfig.Builder::setDescription, DESCRIPTION);
        PARSER.declareField(TrainedModelConfig.Builder::setCreateTime,
            (p, c) -> TimeUtil.parseTimeFieldToInstant(p, CREATE_TIME.getPreferredName()),
            CREATE_TIME,
            ObjectParser.ValueType.VALUE);
        PARSER.declareObject(TrainedModelConfig.Builder::setDefinition,
            (p, c) -> TrainedModelDefinition.fromXContent(p),
            DEFINITION);
        PARSER.declareString(TrainedModelConfig.Builder::setCompressedDefinition, COMPRESSED_DEFINITION);
        PARSER.declareStringArray(TrainedModelConfig.Builder::setTags, TAGS);
        PARSER.declareObject(TrainedModelConfig.Builder::setMetadata, (p, c) -> p.map(), METADATA);
        PARSER.declareObject(TrainedModelConfig.Builder::setInput, (p, c) -> TrainedModelInput.fromXContent(p), INPUT);
        PARSER.declareLong(TrainedModelConfig.Builder::setEstimatedHeapMemory, ESTIMATED_HEAP_MEMORY_USAGE_BYTES);
        PARSER.declareLong(TrainedModelConfig.Builder::setEstimatedOperations, ESTIMATED_OPERATIONS);
        PARSER.declareString(TrainedModelConfig.Builder::setLicenseLevel, LICENSE_LEVEL);
        PARSER.declareObject(TrainedModelConfig.Builder::setDefaultFieldMap, (p, c) -> p.mapStrings(), DEFAULT_FIELD_MAP);
        PARSER.declareNamedObject(TrainedModelConfig.Builder::setInferenceConfig,
            (p, c, n) -> p.namedObject(InferenceConfig.class, n, null),
            INFERENCE_CONFIG);
        PARSER.declareNamedObject(TrainedModelConfig.Builder::setLocation,
            (p, c, n) -> p.namedObject(TrainedModelLocation.class, n, null),
            LOCATION);
    }

    public static TrainedModelConfig fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null).build();
    }

    private final String modelId;
    private final TrainedModelType modelType;
    private final String createdBy;
    private final Version version;
    private final String description;
    private final Instant createTime;
    private final TrainedModelDefinition definition;
    private final String compressedDefinition;
    private final List<String> tags;
    private final Map<String, Object> metadata;
    private final TrainedModelInput input;
    private final Long estimatedHeapMemory;
    private final Long estimatedOperations;
    private final String licenseLevel;
    private final Map<String, String> defaultFieldMap;
    private final InferenceConfig inferenceConfig;
    private final TrainedModelLocation location;

    TrainedModelConfig(String modelId,
                       TrainedModelType modelType,
                       String createdBy,
                       Version version,
                       String description,
                       Instant createTime,
                       TrainedModelDefinition definition,
                       String compressedDefinition,
                       List<String> tags,
                       Map<String, Object> metadata,
                       TrainedModelInput input,
                       Long estimatedHeapMemory,
                       Long estimatedOperations,
                       String licenseLevel,
                       Map<String, String> defaultFieldMap,
                       InferenceConfig inferenceConfig,
                       TrainedModelLocation location) {
        this.modelId = modelId;
        this.modelType = modelType;
        this.createdBy = createdBy;
        this.version = version;
        this.createTime = createTime == null ? null : Instant.ofEpochMilli(createTime.toEpochMilli());
        this.definition = definition;
        this.compressedDefinition = compressedDefinition;
        this.description = description;
        this.tags = tags == null ? null : Collections.unmodifiableList(tags);
        this.metadata = metadata == null ? null : Collections.unmodifiableMap(metadata);
        this.input = input;
        this.estimatedHeapMemory = estimatedHeapMemory;
        this.estimatedOperations = estimatedOperations;
        this.licenseLevel = licenseLevel;
        this.defaultFieldMap = defaultFieldMap == null ? null : Collections.unmodifiableMap(defaultFieldMap);
        this.inferenceConfig = inferenceConfig;
        this.location = location;
    }

    public String getModelId() {
        return modelId;
    }

    public TrainedModelType getModelType() {
        return modelType;
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

    public Instant getCreateTime() {
        return createTime;
    }

    public List<String> getTags() {
        return tags;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public TrainedModelDefinition getDefinition() {
        return definition;
    }

    public String getCompressedDefinition() {
        return compressedDefinition;
    }

    @Nullable
    public TrainedModelLocation getLocation() {
        return location;
    }

    public TrainedModelInput getInput() {
        return input;
    }

    public ByteSizeValue getEstimatedHeapMemory() {
        return estimatedHeapMemory == null ? null : new ByteSizeValue(estimatedHeapMemory);
    }

    public Long getEstimatedHeapMemoryBytes() {
        return estimatedHeapMemory;
    }

    public Long getEstimatedOperations() {
        return estimatedOperations;
    }

    public String getLicenseLevel() {
        return licenseLevel;
    }

    public Map<String, String> getDefaultFieldMap() {
        return defaultFieldMap;
    }

    public InferenceConfig getInferenceConfig() {
        return inferenceConfig;
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
        if (modelType != null) {
            builder.field(MODEL_TYPE.getPreferredName(), modelType.toString());
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
        if (createTime != null) {
            builder.timeField(CREATE_TIME.getPreferredName(), CREATE_TIME.getPreferredName() + "_string", createTime.toEpochMilli());
        }
        if (definition != null) {
            builder.field(DEFINITION.getPreferredName(), definition);
        }
        if (tags != null) {
            builder.field(TAGS.getPreferredName(), tags);
        }
        if (metadata != null) {
            builder.field(METADATA.getPreferredName(), metadata);
        }
        if (input != null) {
            builder.field(INPUT.getPreferredName(), input);
        }
        if (estimatedHeapMemory != null) {
            builder.field(ESTIMATED_HEAP_MEMORY_USAGE_BYTES.getPreferredName(), estimatedHeapMemory);
        }
        if (estimatedOperations != null) {
            builder.field(ESTIMATED_OPERATIONS.getPreferredName(), estimatedOperations);
        }
        if (compressedDefinition != null) {
            builder.field(COMPRESSED_DEFINITION.getPreferredName(), compressedDefinition);
        }
        if (licenseLevel != null) {
            builder.field(LICENSE_LEVEL.getPreferredName(), licenseLevel);
        }
        if (defaultFieldMap != null) {
            builder.field(DEFAULT_FIELD_MAP.getPreferredName(), defaultFieldMap);
        }
        if (inferenceConfig != null) {
            writeNamedObject(builder, params, INFERENCE_CONFIG.getPreferredName(), inferenceConfig);
        }
        if (location != null) {
            writeNamedObject(builder, params, LOCATION.getPreferredName(), location);
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
            Objects.equals(modelType, that.modelType) &&
            Objects.equals(createdBy, that.createdBy) &&
            Objects.equals(version, that.version) &&
            Objects.equals(description, that.description) &&
            Objects.equals(createTime, that.createTime) &&
            Objects.equals(definition, that.definition) &&
            Objects.equals(compressedDefinition, that.compressedDefinition) &&
            Objects.equals(tags, that.tags) &&
            Objects.equals(input, that.input) &&
            Objects.equals(estimatedHeapMemory, that.estimatedHeapMemory) &&
            Objects.equals(estimatedOperations, that.estimatedOperations) &&
            Objects.equals(licenseLevel, that.licenseLevel) &&
            Objects.equals(defaultFieldMap, that.defaultFieldMap) &&
            Objects.equals(inferenceConfig, that.inferenceConfig) &&
            Objects.equals(metadata, that.metadata) &&
            Objects.equals(location, that.location);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId,
            modelType,
            createdBy,
            version,
            createTime,
            definition,
            compressedDefinition,
            description,
            tags,
            estimatedHeapMemory,
            estimatedOperations,
            metadata,
            licenseLevel,
            input,
            inferenceConfig,
            defaultFieldMap,
            location);
    }


    public static class Builder {

        private String modelId;
        private TrainedModelType modelType;
        private String createdBy;
        private Version version;
        private String description;
        private Instant createTime;
        private Map<String, Object> metadata;
        private List<String> tags;
        private TrainedModelDefinition definition;
        private String compressedDefinition;
        private TrainedModelInput input;
        private Long estimatedHeapMemory;
        private Long estimatedOperations;
        private String licenseLevel;
        private Map<String, String> defaultFieldMap;
        private InferenceConfig inferenceConfig;
        private TrainedModelLocation location;

        public Builder setModelId(String modelId) {
            this.modelId = modelId;
            return this;
        }

        public Builder setModelType(String modelType) {
            this.modelType = TrainedModelType.fromString(modelType);
            return this;
        }

        public Builder setModelType(TrainedModelType modelType) {
            this.modelType = modelType;
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

        private Builder setCreateTime(Instant createTime) {
            this.createTime = createTime;
            return this;
        }

        public Builder setTags(List<String> tags) {
            this.tags = tags;
            return this;
        }

        public Builder setTags(String... tags) {
            return setTags(Arrays.asList(tags));
        }

        public Builder setMetadata(Map<String, Object> metadata) {
            this.metadata = metadata;
            return this;
        }

        public Builder setDefinition(TrainedModelDefinition.Builder definition) {
            this.definition = definition == null ? null : definition.build();
            return this;
        }

        public Builder setCompressedDefinition(String compressedDefinition) {
            this.compressedDefinition = compressedDefinition;
            return this;
        }

        public Builder setDefinition(TrainedModelDefinition definition) {
            this.definition = definition;
            return this;
        }

        public Builder setLocation(TrainedModelLocation location) {
            this.location = location;
            return this;
        }

        public Builder setInput(TrainedModelInput input) {
            this.input = input;
            return this;
        }

        private Builder setEstimatedHeapMemory(Long estimatedHeapMemory) {
            this.estimatedHeapMemory = estimatedHeapMemory;
            return this;
        }

        private Builder setEstimatedOperations(Long estimatedOperations) {
            this.estimatedOperations = estimatedOperations;
            return this;
        }

        private Builder setLicenseLevel(String licenseLevel) {
            this.licenseLevel = licenseLevel;
            return this;
        }

        public Builder setDefaultFieldMap(Map<String, String> defaultFieldMap) {
            this.defaultFieldMap = defaultFieldMap;
            return this;
        }

        public Builder setInferenceConfig(InferenceConfig inferenceConfig) {
            this.inferenceConfig = inferenceConfig;
            return this;
        }

        public TrainedModelConfig build() {
            return new TrainedModelConfig(
                modelId,
                modelType,
                createdBy,
                version,
                description,
                createTime,
                definition,
                compressedDefinition,
                tags,
                metadata,
                input,
                estimatedHeapMemory,
                estimatedOperations,
                licenseLevel,
                defaultFieldMap,
                inferenceConfig,
                location);
        }
    }

}
