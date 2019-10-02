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
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LenientlyParsedTrainedModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.StrictlyParsedTrainedModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TrainedModel;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.MlStrings;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObjectHelper;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TrainedModelConfig implements ToXContentObject, Writeable {

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
        parser.declareField(TrainedModelConfig.Builder::setCreatedTime,
            (p, c) -> TimeUtils.parseTimeFieldToInstant(p, CREATED_TIME.getPreferredName()),
            CREATED_TIME,
            ObjectParser.ValueType.VALUE);
        parser.declareLong(TrainedModelConfig.Builder::setModelVersion, MODEL_VERSION);
        parser.declareString(TrainedModelConfig.Builder::setModelType, MODEL_TYPE);
        parser.declareObject(TrainedModelConfig.Builder::setMetadata, (p, c) -> p.map(), METADATA);
        parser.declareNamedObjects(TrainedModelConfig.Builder::setDefinition,
            (p, c, n) -> ignoreUnknownFields ?
                p.namedObject(LenientlyParsedTrainedModel.class, n, null) :
                p.namedObject(StrictlyParsedTrainedModel.class, n, null),
            (modelDocBuilder) -> { /* Noop does not matter as we will throw if more than one is defined */ },
            DEFINITION);
        return parser;
    }

    public static TrainedModelConfig.Builder fromXContent(XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.parse(parser, null) : STRICT_PARSER.parse(parser, null);
    }

    public static String documentId(String modelId, long modelVersion) {
        return NAME + "-" + modelId + "-" + modelVersion;
    }


    private final String modelId;
    private final String createdBy;
    private final Version version;
    private final String description;
    private final Instant createdTime;
    private final long modelVersion;
    private final String modelType;
    private final Map<String, Object> metadata;
    // TODO how to reference and store large models that will not be executed in Java???
    // Potentially allow this to be null and have an {index: indexName, doc: model_doc_id} or something
    // TODO Should this be lazily parsed when loading via the index???
    private final TrainedModel definition;
    TrainedModelConfig(String modelId,
                       String createdBy,
                       Version version,
                       String description,
                       Instant createdTime,
                       Long modelVersion,
                       String modelType,
                       TrainedModel definition,
                       Map<String, Object> metadata) {
        this.modelId = ExceptionsHelper.requireNonNull(modelId, MODEL_ID);
        this.createdBy = ExceptionsHelper.requireNonNull(createdBy, CREATED_BY);
        this.version = ExceptionsHelper.requireNonNull(version, VERSION);
        this.createdTime = Instant.ofEpochMilli(ExceptionsHelper.requireNonNull(createdTime, CREATED_TIME).toEpochMilli());
        this.modelType = ExceptionsHelper.requireNonNull(modelType, MODEL_TYPE);
        this.definition = definition;
        this.description = description;
        this.metadata = metadata == null ? null : Collections.unmodifiableMap(metadata);
        this.modelVersion = modelVersion == null ? 0 : modelVersion;
    }

    public TrainedModelConfig(StreamInput in) throws IOException {
        modelId = in.readString();
        createdBy = in.readString();
        version = Version.readVersion(in);
        description = in.readOptionalString();
        createdTime = in.readInstant();
        modelVersion = in.readVLong();
        modelType = in.readString();
        definition = in.readOptionalNamedWriteable(TrainedModel.class);
        metadata = in.readMap();
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

    public long getModelVersion() {
        return modelVersion;
    }

    public String getModelType() {
        return modelType;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    @Nullable
    public TrainedModel getDefinition() {
        return definition;
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
        out.writeInstant(createdTime);
        out.writeVLong(modelVersion);
        out.writeString(modelType);
        out.writeOptionalNamedWriteable(definition);
        out.writeMap(metadata);
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
        builder.timeField(CREATED_TIME.getPreferredName(), CREATED_TIME.getPreferredName() + "_string", createdTime.toEpochMilli());
        builder.field(MODEL_VERSION.getPreferredName(), modelVersion);
        builder.field(MODEL_TYPE.getPreferredName(), modelType);
        if (definition != null) {
            NamedXContentObjectHelper.writeNamedObjects(builder,
                params,
                false,
                DEFINITION.getPreferredName(),
                Collections.singletonList(definition));
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
        private TrainedModel definition;

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

        public Builder setCreatedTime(Instant createdTime) {
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

        public Builder setDefinition(TrainedModel definition) {
            this.definition = definition;
            return this;
        }

        private Builder setDefinition(List<TrainedModel> definition) {
            if (definition.size() != 1) {
                throw ExceptionsHelper.badRequestException("[{}] must have exactly one trained model defined.",
                    DEFINITION.getPreferredName());
            }
            return setDefinition(definition.get(0));
        }

        // TODO move to REST level instead of here in the builder
        public void validate() {
            // We require a definition to be available until we support other means of supplying the definition
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

            if (createdTime != null) {
                throw ExceptionsHelper.badRequestException("illegal to set [{}] at inference model creation",
                    CREATED_TIME.getPreferredName());
            }
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
                definition,
                metadata);
        }

        public TrainedModelConfig build(Version version) {
            return new TrainedModelConfig(
                modelId,
                createdBy,
                version,
                description,
                Instant.now(),
                modelVersion,
                modelType,
                definition,
                metadata);
        }
    }
}
