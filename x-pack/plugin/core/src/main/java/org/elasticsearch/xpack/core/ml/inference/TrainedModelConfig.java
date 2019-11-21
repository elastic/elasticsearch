/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference;

import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.common.time.TimeUtils;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.MlStrings;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TrainedModelConfig implements ToXContentObject, Writeable {

    public static final String NAME = "trained_model_config";

    private static final String ESTIMATED_HEAP_MEMORY_USAGE_HUMAN = "estimated_heap_memory_usage";

    public static final ParseField MODEL_ID = new ParseField("model_id");
    public static final ParseField CREATED_BY = new ParseField("created_by");
    public static final ParseField VERSION = new ParseField("version");
    public static final ParseField DESCRIPTION = new ParseField("description");
    public static final ParseField CREATE_TIME = new ParseField("create_time");
    public static final ParseField DEFINITION = new ParseField("definition");
    public static final ParseField TAGS = new ParseField("tags");
    public static final ParseField METADATA = new ParseField("metadata");
    public static final ParseField INPUT = new ParseField("input");
    public static final ParseField ESTIMATED_HEAP_MEMORY_USAGE_BYTES = new ParseField("estimated_heap_memory_usage_bytes");
    public static final ParseField ESTIMATED_OPERATIONS = new ParseField("estimated_operations");
    public static final ParseField LICENSE_LEVEL = new ParseField("license_level");

    // These parsers follow the pattern that metadata is parsed leniently (to allow for enhancements), whilst config is parsed strictly
    public static final ObjectParser<TrainedModelConfig.Builder, Void> LENIENT_PARSER = createParser(true);
    public static final ObjectParser<TrainedModelConfig.Builder, Void> STRICT_PARSER = createParser(false);

    private static ObjectParser<TrainedModelConfig.Builder, Void> createParser(boolean ignoreUnknownFields) {
        ObjectParser<TrainedModelConfig.Builder, Void> parser = new ObjectParser<>(NAME,
            ignoreUnknownFields,
            TrainedModelConfig.Builder::new);
        parser.declareString(TrainedModelConfig.Builder::setModelId, MODEL_ID);
        parser.declareString(TrainedModelConfig.Builder::setCreatedBy, CREATED_BY);
        parser.declareString(TrainedModelConfig.Builder::setVersion, VERSION);
        parser.declareString(TrainedModelConfig.Builder::setDescription, DESCRIPTION);
        parser.declareField(TrainedModelConfig.Builder::setCreateTime,
            (p, c) -> TimeUtils.parseTimeFieldToInstant(p, CREATE_TIME.getPreferredName()),
            CREATE_TIME,
            ObjectParser.ValueType.VALUE);
        parser.declareStringArray(TrainedModelConfig.Builder::setTags, TAGS);
        parser.declareObject(TrainedModelConfig.Builder::setMetadata, (p, c) -> p.map(), METADATA);
        parser.declareString((trainedModelConfig, s) -> {}, InferenceIndexConstants.DOC_TYPE);
        parser.declareObject(TrainedModelConfig.Builder::setInput,
            (p, c) -> TrainedModelInput.fromXContent(p, ignoreUnknownFields),
            INPUT);
        parser.declareLong(TrainedModelConfig.Builder::setEstimatedHeapMemory, ESTIMATED_HEAP_MEMORY_USAGE_BYTES);
        parser.declareLong(TrainedModelConfig.Builder::setEstimatedOperations, ESTIMATED_OPERATIONS);
        parser.declareString(TrainedModelConfig.Builder::setLicenseLevel, LICENSE_LEVEL);
        return parser;
    }

    public static TrainedModelConfig.Builder fromXContent(XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.parse(parser, null) : STRICT_PARSER.parse(parser, null);
    }

    private final String modelId;
    private final String createdBy;
    private final Version version;
    private final String description;
    private final Instant createTime;
    private final List<String> tags;
    private final Map<String, Object> metadata;
    private final TrainedModelInput input;
    private final long estimatedHeapMemory;
    private final long estimatedOperations;
    private final License.OperationMode licenseLevel;

    private final TrainedModelDefinition definition;

    TrainedModelConfig(String modelId,
                       String createdBy,
                       Version version,
                       String description,
                       Instant createTime,
                       TrainedModelDefinition definition,
                       List<String> tags,
                       Map<String, Object> metadata,
                       TrainedModelInput input,
                       Long estimatedHeapMemory,
                       Long estimatedOperations,
                       String licenseLevel) {
        this.modelId = ExceptionsHelper.requireNonNull(modelId, MODEL_ID);
        this.createdBy = ExceptionsHelper.requireNonNull(createdBy, CREATED_BY);
        this.version = ExceptionsHelper.requireNonNull(version, VERSION);
        this.createTime = Instant.ofEpochMilli(ExceptionsHelper.requireNonNull(createTime, CREATE_TIME).toEpochMilli());
        this.definition = definition;
        this.description = description;
        this.tags = Collections.unmodifiableList(ExceptionsHelper.requireNonNull(tags, TAGS));
        this.metadata = metadata == null ? null : Collections.unmodifiableMap(metadata);
        this.input = ExceptionsHelper.requireNonNull(input, INPUT);
        if (ExceptionsHelper.requireNonNull(estimatedHeapMemory, ESTIMATED_HEAP_MEMORY_USAGE_BYTES) < 0) {
            throw new IllegalArgumentException(
                "[" + ESTIMATED_HEAP_MEMORY_USAGE_BYTES.getPreferredName() + "] must be greater than or equal to 0");
        }
        this.estimatedHeapMemory = estimatedHeapMemory;
        if (ExceptionsHelper.requireNonNull(estimatedOperations, ESTIMATED_OPERATIONS) < 0) {
            throw new IllegalArgumentException("[" + ESTIMATED_OPERATIONS.getPreferredName() + "] must be greater than or equal to 0");
        }
        this.estimatedOperations = estimatedOperations;
        this.licenseLevel = License.OperationMode.resolve(ExceptionsHelper.requireNonNull(licenseLevel, LICENSE_LEVEL));
    }

    public TrainedModelConfig(StreamInput in) throws IOException {
        modelId = in.readString();
        createdBy = in.readString();
        version = Version.readVersion(in);
        description = in.readOptionalString();
        createTime = in.readInstant();
        definition = in.readOptionalWriteable(TrainedModelDefinition::new);
        tags = Collections.unmodifiableList(in.readList(StreamInput::readString));
        metadata = in.readMap();
        input = new TrainedModelInput(in);
        estimatedHeapMemory = in.readVLong();
        estimatedOperations = in.readVLong();
        licenseLevel = License.OperationMode.resolve(in.readString());
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

    public Instant getCreateTime() {
        return createTime;
    }

    public List<String> getTags() {
        return tags;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    @Nullable
    public TrainedModelDefinition getDefinition() {
        return definition;
    }

    public TrainedModelInput getInput() {
        return input;
    }

    public static Builder builder() {
        return new Builder();
    }

    public long getEstimatedHeapMemory() {
        return estimatedHeapMemory;
    }

    public long getEstimatedOperations() {
        return estimatedOperations;
    }

    public License.OperationMode getLicenseLevel() {
        return licenseLevel;
    }

    public boolean isAvailableWithLicense(XPackLicenseState licenseState) {
        // Basic is always true
        if (licenseLevel.equals(License.OperationMode.BASIC)) {
            return true;
        }

        // The model license does not matter, this is the highest licensed level
        if (licenseState.isActive() && XPackLicenseState.isPlatinumOrTrialOperationMode(licenseState.getOperationMode())) {
            return true;
        }

        // catch the rest, if the license is active and is at least the required model license
        return licenseState.isActive() && License.OperationMode.compare(licenseState.getOperationMode(), licenseLevel) >= 0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        out.writeString(createdBy);
        Version.writeVersion(version, out);
        out.writeOptionalString(description);
        out.writeInstant(createTime);
        out.writeOptionalWriteable(definition);
        out.writeCollection(tags, StreamOutput::writeString);
        out.writeMap(metadata);
        input.writeTo(out);
        out.writeVLong(estimatedHeapMemory);
        out.writeVLong(estimatedOperations);
        out.writeString(licenseLevel.description());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MODEL_ID.getPreferredName(), modelId);
        builder.field(CREATED_BY.getPreferredName(), createdBy);
        builder.field(VERSION.getPreferredName(), version.toString());
        if (description != null) {
            builder.field(DESCRIPTION.getPreferredName(), description);
        }
        builder.timeField(CREATE_TIME.getPreferredName(), CREATE_TIME.getPreferredName() + "_string", createTime.toEpochMilli());
        // We don't store the definition in the same document as the configuration
        if ((params.paramAsBoolean(ToXContentParams.FOR_INTERNAL_STORAGE, false) == false) && definition != null) {
            builder.field(DEFINITION.getPreferredName(), definition);
        }
        builder.field(TAGS.getPreferredName(), tags);
        if (metadata != null) {
            builder.field(METADATA.getPreferredName(), metadata);
        }
        if (params.paramAsBoolean(ToXContentParams.FOR_INTERNAL_STORAGE, false)) {
            builder.field(InferenceIndexConstants.DOC_TYPE.getPreferredName(), NAME);
        }
        builder.field(INPUT.getPreferredName(), input);
        builder.humanReadableField(
            ESTIMATED_HEAP_MEMORY_USAGE_BYTES.getPreferredName(),
            ESTIMATED_HEAP_MEMORY_USAGE_HUMAN,
            new ByteSizeValue(estimatedHeapMemory));
        builder.field(ESTIMATED_OPERATIONS.getPreferredName(), estimatedOperations);
        builder.field(LICENSE_LEVEL.getPreferredName(), licenseLevel.description());
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
            Objects.equals(createTime, that.createTime) &&
            Objects.equals(definition, that.definition) &&
            Objects.equals(tags, that.tags) &&
            Objects.equals(input, that.input) &&
            Objects.equals(estimatedHeapMemory, that.estimatedHeapMemory) &&
            Objects.equals(estimatedOperations, that.estimatedOperations) &&
            Objects.equals(licenseLevel, that.licenseLevel) &&
            Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId,
            createdBy,
            version,
            createTime,
            definition,
            description,
            tags,
            metadata,
            estimatedHeapMemory,
            estimatedOperations,
            input,
            licenseLevel);
    }

    public static class Builder {

        private String modelId;
        private String createdBy;
        private Version version;
        private String description;
        private Instant createTime;
        private List<String> tags = Collections.emptyList();
        private Map<String, Object> metadata;
        private TrainedModelInput input;
        private TrainedModelDefinition definition;
        private Long estimatedHeapMemory;
        private Long estimatedOperations;
        private String licenseLevel = License.OperationMode.PLATINUM.description();

        public Builder setModelId(String modelId) {
            this.modelId = modelId;
            return this;
        }

        public Builder setCreatedBy(String createdBy) {
            this.createdBy = createdBy;
            return this;
        }

        public Builder setVersion(Version version) {
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

        public Builder setCreateTime(Instant createTime) {
            this.createTime = createTime;
            return this;
        }

        public Builder setTags(List<String> tags) {
            this.tags = ExceptionsHelper.requireNonNull(tags, TAGS);
            return this;
        }

        public Builder setMetadata(Map<String, Object> metadata) {
            this.metadata = metadata;
            return this;
        }

        public Builder setDefinition(TrainedModelDefinition.Builder definition) {
            this.definition = definition.build();
            return this;
        }

        public Builder setDefinition(TrainedModelDefinition definition) {
            this.definition = definition;
            return this;
        }

        public Builder setInput(TrainedModelInput input) {
            this.input = input;
            return this;
        }

        public Builder setEstimatedHeapMemory(long estimatedHeapMemory) {
            this.estimatedHeapMemory = estimatedHeapMemory;
            return this;
        }

        public Builder setEstimatedOperations(long estimatedOperations) {
            this.estimatedOperations = estimatedOperations;
            return this;
        }

        public Builder setLicenseLevel(String licenseLevel) {
            this.licenseLevel = licenseLevel;
            return this;
        }

        // TODO move to REST level instead of here in the builder
        public void validate() {
            // We require a definition to be available here even though it will be stored in a different doc
            ExceptionsHelper.requireNonNull(definition, DEFINITION);
            ExceptionsHelper.requireNonNull(modelId, MODEL_ID);

            if (MlStrings.isValidId(modelId) == false) {
                throw ExceptionsHelper.badRequestException(Messages.getMessage(Messages.INVALID_ID, MODEL_ID.getPreferredName(), modelId));
            }

            if (MlStrings.hasValidLengthForId(modelId) == false) {
                throw ExceptionsHelper.badRequestException(Messages.getMessage(Messages.ID_TOO_LONG,
                    MODEL_ID.getPreferredName(),
                    modelId,
                    MlStrings.ID_LENGTH_LIMIT));
            }

            checkIllegalSetting(version, VERSION.getPreferredName());
            checkIllegalSetting(createdBy, CREATED_BY.getPreferredName());
            checkIllegalSetting(createTime, CREATE_TIME.getPreferredName());
            checkIllegalSetting(estimatedHeapMemory, ESTIMATED_HEAP_MEMORY_USAGE_BYTES.getPreferredName());
            checkIllegalSetting(estimatedOperations, ESTIMATED_OPERATIONS.getPreferredName());
            checkIllegalSetting(licenseLevel, LICENSE_LEVEL.getPreferredName());
        }

        private static void checkIllegalSetting(Object value, String setting) {
            if (value != null) {
                throw ExceptionsHelper.badRequestException("illegal to set [{}] at inference model creation", setting);
            }
        }

        public TrainedModelConfig build() {
            return new TrainedModelConfig(
                modelId,
                createdBy,
                version,
                description,
                createTime == null ? Instant.now() : createTime,
                definition,
                tags,
                metadata,
                input,
                estimatedHeapMemory,
                estimatedOperations,
                licenseLevel);
        }
    }

}
