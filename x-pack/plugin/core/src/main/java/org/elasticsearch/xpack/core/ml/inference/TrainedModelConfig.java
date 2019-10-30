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
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
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

    public static final ParseField MODEL_ID = new ParseField("model_id");
    public static final ParseField CREATED_BY = new ParseField("created_by");
    public static final ParseField VERSION = new ParseField("version");
    public static final ParseField DESCRIPTION = new ParseField("description");
    public static final ParseField CREATE_TIME = new ParseField("create_time");
    public static final ParseField DEFINITION = new ParseField("definition");
    public static final ParseField TAGS = new ParseField("tags");
    public static final ParseField METADATA = new ParseField("metadata");
    public static final ParseField INPUT = new ParseField("input");

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

    private final TrainedModelDefinition definition;

    TrainedModelConfig(String modelId,
                       String createdBy,
                       Version version,
                       String description,
                       Instant createTime,
                       TrainedModelDefinition definition,
                       List<String> tags,
                       Map<String, Object> metadata,
                       TrainedModelInput input) {
        this.modelId = ExceptionsHelper.requireNonNull(modelId, MODEL_ID);
        this.createdBy = ExceptionsHelper.requireNonNull(createdBy, CREATED_BY);
        this.version = ExceptionsHelper.requireNonNull(version, VERSION);
        this.createTime = Instant.ofEpochMilli(ExceptionsHelper.requireNonNull(createTime, CREATE_TIME).toEpochMilli());
        this.definition = definition;
        this.description = description;
        this.tags = Collections.unmodifiableList(ExceptionsHelper.requireNonNull(tags, TAGS));
        this.metadata = metadata == null ? null : Collections.unmodifiableMap(metadata);
        this.input = ExceptionsHelper.requireNonNull(input, INPUT);
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
            input);
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

            if (version != null) {
                throw ExceptionsHelper.badRequestException("illegal to set [{}] at inference model creation", VERSION.getPreferredName());
            }

            if (createdBy != null) {
                throw ExceptionsHelper.badRequestException("illegal to set [{}] at inference model creation",
                    CREATED_BY.getPreferredName());
            }

            if (createTime != null) {
                throw ExceptionsHelper.badRequestException("illegal to set [{}] at inference model creation",
                    CREATE_TIME.getPreferredName());
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
                input);
        }
    }

}
