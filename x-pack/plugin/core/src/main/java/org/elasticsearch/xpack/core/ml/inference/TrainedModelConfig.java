/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.license.License;
import org.elasticsearch.xpack.core.common.time.TimeUtils;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LenientlyParsedInferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LenientlyParsedTrainedModelLocation;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.StrictlyParsedInferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.StrictlyParsedTrainedModelLocation;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TrainedModelLocation;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.metadata.FeatureImportanceBaseline;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.metadata.Hyperparameters;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.metadata.TotalFeatureImportance;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.MlStrings;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xpack.core.ml.utils.NamedXContentObjectHelper.writeNamedObject;
import static org.elasticsearch.xpack.core.ml.utils.ToXContentParams.EXCLUDE_GENERATED;


public class TrainedModelConfig implements ToXContentObject, Writeable {

    public static final String NAME = "trained_model_config";
    public static final int CURRENT_DEFINITION_COMPRESSION_VERSION = 1;
    public static final String DECOMPRESS_DEFINITION = "decompress_definition";
    public static final String TOTAL_FEATURE_IMPORTANCE = "total_feature_importance";
    public static final String FEATURE_IMPORTANCE_BASELINE = "feature_importance_baseline";
    public static final String HYPERPARAMETERS = "hyperparameters";
    public static final String MODEL_ALIASES = "model_aliases";

    private static final String ESTIMATED_HEAP_MEMORY_USAGE_HUMAN = "estimated_heap_memory_usage";

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

    public static final Version VERSION_3RD_PARTY_CONFIG_ADDED = Version.V_8_0_0;

    // These parsers follow the pattern that metadata is parsed leniently (to allow for enhancements), whilst config is parsed strictly
    public static final ObjectParser<TrainedModelConfig.Builder, Void> LENIENT_PARSER = createParser(true);
    public static final ObjectParser<TrainedModelConfig.Builder, Void> STRICT_PARSER = createParser(false);

    private static ObjectParser<TrainedModelConfig.Builder, Void> createParser(boolean ignoreUnknownFields) {
        ObjectParser<TrainedModelConfig.Builder, Void> parser = new ObjectParser<>(NAME,
            ignoreUnknownFields,
            TrainedModelConfig.Builder::new);
        parser.declareString(TrainedModelConfig.Builder::setModelId, MODEL_ID);
        parser.declareString(TrainedModelConfig.Builder::setModelType, MODEL_TYPE);
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
        parser.declareObject(TrainedModelConfig.Builder::setLazyDefinition,
            (p, c) -> TrainedModelDefinition.fromXContent(p, ignoreUnknownFields),
            DEFINITION);
        parser.declareString(TrainedModelConfig.Builder::setLazyDefinition, COMPRESSED_DEFINITION);
        parser.declareString(TrainedModelConfig.Builder::setLicenseLevel, LICENSE_LEVEL);
        parser.declareObject(TrainedModelConfig.Builder::setDefaultFieldMap, (p, c) -> p.mapStrings(), DEFAULT_FIELD_MAP);
        parser.declareNamedObject(TrainedModelConfig.Builder::setInferenceConfig, (p, c, n) -> ignoreUnknownFields ?
            p.namedObject(LenientlyParsedInferenceConfig.class, n, null) :
            p.namedObject(StrictlyParsedInferenceConfig.class, n, null),
            INFERENCE_CONFIG);
        parser.declareNamedObject(TrainedModelConfig.Builder::setLocation,
            (p, c, n) -> ignoreUnknownFields ?
                p.namedObject(LenientlyParsedTrainedModelLocation.class, n, null) :
                p.namedObject(StrictlyParsedTrainedModelLocation.class, n, null),
            LOCATION);
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
    private final TrainedModelType modelType;
    private final List<String> tags;
    private final Map<String, Object> metadata;
    private final TrainedModelInput input;
    private final long estimatedHeapMemory;
    private final long estimatedOperations;
    private final License.OperationMode licenseLevel;
    private final Map<String, String> defaultFieldMap;
    private final InferenceConfig inferenceConfig;

    private final LazyModelDefinition definition;
    private final TrainedModelLocation location;

    TrainedModelConfig(String modelId,
                       TrainedModelType modelType,
                       String createdBy,
                       Version version,
                       String description,
                       Instant createTime,
                       LazyModelDefinition definition,
                       List<String> tags,
                       Map<String, Object> metadata,
                       TrainedModelInput input,
                       Long estimatedHeapMemory,
                       Long estimatedOperations,
                       String licenseLevel,
                       Map<String, String> defaultFieldMap,
                       InferenceConfig inferenceConfig,
                       TrainedModelLocation location) {
        this.modelId = ExceptionsHelper.requireNonNull(modelId, MODEL_ID);
        this.modelType = modelType;
        this.createdBy = ExceptionsHelper.requireNonNull(createdBy, CREATED_BY);
        this.version = ExceptionsHelper.requireNonNull(version, VERSION);
        this.createTime = Instant.ofEpochMilli(ExceptionsHelper.requireNonNull(createTime, CREATE_TIME).toEpochMilli());
        this.definition = definition;
        this.description = description;
        this.tags = Collections.unmodifiableList(ExceptionsHelper.requireNonNull(tags, TAGS));
        this.metadata = metadata == null ? null : Collections.unmodifiableMap(metadata);
        this.input = ExceptionsHelper.requireNonNull(handleDefaultInput(input, modelType), INPUT);
        if (ExceptionsHelper.requireNonNull(estimatedHeapMemory, ESTIMATED_HEAP_MEMORY_USAGE_BYTES) < 0) {
            throw new IllegalArgumentException(
                "[" + ESTIMATED_HEAP_MEMORY_USAGE_BYTES.getPreferredName() + "] must be greater than or equal to 0");
        }
        this.estimatedHeapMemory = estimatedHeapMemory;
        if (ExceptionsHelper.requireNonNull(estimatedOperations, ESTIMATED_OPERATIONS) < 0) {
            throw new IllegalArgumentException("[" + ESTIMATED_OPERATIONS.getPreferredName() + "] must be greater than or equal to 0");
        }
        this.estimatedOperations = estimatedOperations;
        this.licenseLevel = License.OperationMode.parse(ExceptionsHelper.requireNonNull(licenseLevel, LICENSE_LEVEL));
        this.defaultFieldMap = defaultFieldMap == null ? null : Collections.unmodifiableMap(defaultFieldMap);
        this.inferenceConfig = inferenceConfig;
        this.location = location;
    }

    private static TrainedModelInput handleDefaultInput(TrainedModelInput input, TrainedModelType modelType) {
        if (modelType == null) {
            return input;
        }
        return input == null ? modelType.getDefaultInput() : input;
    }

    public TrainedModelConfig(StreamInput in) throws IOException {
        modelId = in.readString();
        createdBy = in.readString();
        version = Version.readVersion(in);
        description = in.readOptionalString();
        createTime = in.readInstant();
        definition = in.readOptionalWriteable(LazyModelDefinition::fromStreamInput);
        tags = Collections.unmodifiableList(in.readList(StreamInput::readString));
        metadata = in.readMap();
        input = new TrainedModelInput(in);
        estimatedHeapMemory = in.readVLong();
        estimatedOperations = in.readVLong();
        licenseLevel = License.OperationMode.parse(in.readString());
        this.defaultFieldMap = in.readBoolean() ?
            Collections.unmodifiableMap(in.readMap(StreamInput::readString, StreamInput::readString)) :
            null;

        this.inferenceConfig = in.readOptionalNamedWriteable(InferenceConfig.class);
        if (in.getVersion().onOrAfter(VERSION_3RD_PARTY_CONFIG_ADDED)) {
            this.modelType = in.readOptionalEnum(TrainedModelType.class);
            this.location = in.readOptionalNamedWriteable(TrainedModelLocation.class);
        } else {
            this.modelType = null;
            this.location = null;
        }
    }

    public String getModelId() {
        return modelId;
    }

    @Nullable
    public TrainedModelType getModelType() {
        return this.modelType;
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

    public Map<String, String> getDefaultFieldMap() {
        return defaultFieldMap;
    }

    @Nullable
    public InferenceConfig getInferenceConfig() {
        return inferenceConfig;
    }

    @Nullable
    public BytesReference getCompressedDefinition() throws IOException {
        if (definition == null) {
            return null;
        }
        return definition.getCompressedDefinition();
    }

    public void clearCompressed() {
        definition.compressedRepresentation = null;
    }

    public TrainedModelConfig ensureParsedDefinition(NamedXContentRegistry xContentRegistry) throws IOException {
        if (definition == null) {
            return null;
        }
        definition.ensureParsedDefinition(xContentRegistry);
        return this;
    }

    public TrainedModelConfig ensureParsedDefinitionUnsafe(NamedXContentRegistry xContentRegistry) throws IOException {
        if (definition == null) {
            return null;
        }
        definition.ensureParsedDefinitionUnsafe(xContentRegistry);
        return this;
    }

    @Nullable
    public TrainedModelDefinition getModelDefinition() {
        if (definition == null) {
            return null;
        }
        return definition.parsedDefinition;
    }

    @Nullable
    public TrainedModelLocation getLocation() {
        return location;
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
        if (defaultFieldMap != null) {
            out.writeBoolean(true);
            out.writeMap(defaultFieldMap, StreamOutput::writeString, StreamOutput::writeString);
        } else {
            out.writeBoolean(false);
        }
        out.writeOptionalNamedWriteable(inferenceConfig);
        if (out.getVersion().onOrAfter(VERSION_3RD_PARTY_CONFIG_ADDED)) {
            out.writeOptionalEnum(modelType);
            out.writeOptionalNamedWriteable(location);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MODEL_ID.getPreferredName(), modelId);
        if (modelType != null) {
            builder.field(MODEL_TYPE.getPreferredName(), modelType.toString());
        }
        // If the model is to be exported for future import to another cluster, these fields are irrelevant.
        if (params.paramAsBoolean(EXCLUDE_GENERATED, false) == false) {
            builder.field(CREATED_BY.getPreferredName(), createdBy);
            builder.field(VERSION.getPreferredName(), version.toString());
            builder.timeField(CREATE_TIME.getPreferredName(), CREATE_TIME.getPreferredName() + "_string", createTime.toEpochMilli());
            builder.humanReadableField(
                ESTIMATED_HEAP_MEMORY_USAGE_BYTES.getPreferredName(),
                ESTIMATED_HEAP_MEMORY_USAGE_HUMAN,
                ByteSizeValue.ofBytes(estimatedHeapMemory));
            builder.field(ESTIMATED_OPERATIONS.getPreferredName(), estimatedOperations);
            builder.field(LICENSE_LEVEL.getPreferredName(), licenseLevel.description());
        }
        if (description != null) {
            builder.field(DESCRIPTION.getPreferredName(), description);
        }
        // We don't store the definition in the same document as the configuration
        if ((params.paramAsBoolean(ToXContentParams.FOR_INTERNAL_STORAGE, false) == false) && definition != null) {
            if (params.paramAsBoolean(DECOMPRESS_DEFINITION, false)) {
                builder.field(DEFINITION.getPreferredName(), definition);
            } else {
                builder.field(COMPRESSED_DEFINITION.getPreferredName(), definition.getBase64CompressedDefinition());
            }
        }
        builder.field(TAGS.getPreferredName(), tags);
        if (metadata != null) {
            builder.field(METADATA.getPreferredName(), metadata);
        }
        if (params.paramAsBoolean(ToXContentParams.FOR_INTERNAL_STORAGE, false)) {
            builder.field(InferenceIndexConstants.DOC_TYPE.getPreferredName(), NAME);
        }
        builder.field(INPUT.getPreferredName(), input);
        if (defaultFieldMap != null && defaultFieldMap.isEmpty() == false) {
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
            description,
            tags,
            metadata,
            estimatedHeapMemory,
            estimatedOperations,
            input,
            licenseLevel,
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
        private List<String> tags = Collections.emptyList();
        private Map<String, Object> metadata;
        private TrainedModelInput input;
        private Long estimatedHeapMemory;
        private Long estimatedOperations;
        private LazyModelDefinition definition;
        private String licenseLevel;
        private Map<String, String> defaultFieldMap;
        private InferenceConfig inferenceConfig;
        private TrainedModelLocation location;

        public Builder() {}

        public Builder(TrainedModelConfig config) {
            this.modelId = config.getModelId();
            this.modelType = config.getModelType();
            this.createdBy = config.getCreatedBy();
            this.version = config.getVersion();
            this.createTime = config.getCreateTime();
            this.definition = config.definition == null ? null : new LazyModelDefinition(config.definition);
            this.description = config.getDescription();
            this.tags = config.getTags();
            this.metadata = config.getMetadata() == null ? null : new HashMap<>(config.getMetadata());
            this.input = config.getInput();
            this.estimatedOperations = config.estimatedOperations;
            this.estimatedHeapMemory = config.estimatedHeapMemory;
            this.licenseLevel = config.licenseLevel.description();
            this.defaultFieldMap = config.defaultFieldMap == null ? null : new HashMap<>(config.defaultFieldMap);
            this.inferenceConfig = config.inferenceConfig;
            this.location = config.location;
        }

        public Builder setModelId(String modelId) {
            this.modelId = modelId;
            return this;
        }

        public TrainedModelType getModelType() {
            return modelType;
        }

        private Builder setModelType(String modelType) {
            this.modelType = TrainedModelType.fromString(modelType);
            return this;
        }

        public Builder setModelType(TrainedModelType modelType) {
            this.modelType = modelType;
            return this;
        }

        public String getModelId() {
            return this.modelId;
        }

        public Builder setCreatedBy(String createdBy) {
            this.createdBy = createdBy;
            return this;
        }

        public Version getVersion() {
            return version;
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

        public Builder setFeatureImportance(List<TotalFeatureImportance> totalFeatureImportance) {
            if (totalFeatureImportance == null) {
                return this;
            }
            return addToMetadata(
                TOTAL_FEATURE_IMPORTANCE,
                totalFeatureImportance.stream().map(TotalFeatureImportance::asMap).collect(Collectors.toList())
            );
        }

        public Builder setBaselineFeatureImportance(FeatureImportanceBaseline featureImportanceBaseline) {
            if (featureImportanceBaseline == null) {
                return this;
            }
            return addToMetadata(FEATURE_IMPORTANCE_BASELINE, featureImportanceBaseline.asMap());
        }

        public Builder setHyperparameters(List<Hyperparameters> hyperparameters) {
            if (hyperparameters == null) {
                return this;
            }
            return addToMetadata(
                HYPERPARAMETERS,
                hyperparameters.stream().map(Hyperparameters::asMap).collect(Collectors.toList())
            );
        }

        public Builder setModelAliases(Set<String> modelAliases) {
            if (modelAliases == null || modelAliases.isEmpty()) {
                return this;
            }
            return addToMetadata(MODEL_ALIASES, modelAliases.stream().sorted().collect(Collectors.toList()));
        }

        private Builder addToMetadata(String fieldName, Object value) {
            if (this.metadata == null) {
                this.metadata = new HashMap<>();
            }
            this.metadata.put(fieldName, value);
            return this;
        }

        public Builder setParsedDefinition(TrainedModelDefinition.Builder definition) {
            if (definition == null) {
                return this;
            }
            this.definition = LazyModelDefinition.fromParsedDefinition(definition.build());
            return this;
        }

        public Builder setDefinitionFromBytes(BytesReference definition) {
            if (definition == null) {
                return this;
            }
            this.definition = LazyModelDefinition.fromCompressedData(definition);
            return this;
        }

        public Builder clearDefinition() {
            this.definition = null;
            return this;
        }

        private Builder setLazyDefinition(TrainedModelDefinition.Builder parsedTrainedModel) {
            if (parsedTrainedModel == null) {
                return this;
            }

            if (this.definition != null) {
                throw new IllegalArgumentException(new ParameterizedMessage(
                    "both [{}] and [{}] cannot be set.",
                    COMPRESSED_DEFINITION.getPreferredName(),
                    DEFINITION.getPreferredName())
                    .getFormattedMessage());
            }
            this.definition = LazyModelDefinition.fromParsedDefinition(parsedTrainedModel.build());
            return this;
        }

        private Builder setLazyDefinition(String compressedString) {
            if (compressedString == null) {
                return this;
            }

            if (this.definition != null) {
                throw new IllegalArgumentException(new ParameterizedMessage(
                    "both [{}] and [{}] cannot be set.",
                    COMPRESSED_DEFINITION.getPreferredName(),
                    DEFINITION.getPreferredName())
                    .getFormattedMessage());
            }
            this.definition = LazyModelDefinition.fromBase64String(compressedString);
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

        public Builder setDefaultFieldMap(Map<String, String> defaultFieldMap) {
            this.defaultFieldMap = defaultFieldMap;
            return this;
        }

        public Builder setInferenceConfig(InferenceConfig inferenceConfig) {
            this.inferenceConfig = inferenceConfig;
            return this;
        }

        public Builder validate() {
            return validate(false);
        }

        /**
         * Runs validations against the builder.
         * @return The current builder object if validations are successful
         * @throws ActionRequestValidationException when there are validation failures.
         */
        public Builder validate(boolean forCreation) {
            // We require a definition to be available here even though it will be stored in a different doc
            ActionRequestValidationException validationException = null;
            if (definition == null && location == null) {
                validationException = addValidationError("either a model [" + DEFINITION.getPreferredName() + "] " +
                    "or [" + LOCATION.getPreferredName() + "] must be defined.", validationException);
            }
            if (definition != null && location != null) {
                validationException = addValidationError("[" + DEFINITION.getPreferredName() + "] " +
                    "and [" + LOCATION.getPreferredName() + "] are both defined but only one can be used.", validationException);
            }
            if (definition == null && modelType == null) {
                validationException = addValidationError("[" + MODEL_TYPE.getPreferredName() + "] must be set if " +
                    "[" + DEFINITION.getPreferredName() + "] is not defined.", validationException);
            }
            if (modelId == null) {
                validationException = addValidationError("[" + MODEL_ID.getPreferredName() + "] must not be null.", validationException);
            }
            if (inferenceConfig == null && forCreation) {
                validationException = addValidationError("[" + INFERENCE_CONFIG.getPreferredName() + "] must not be null.",
                    validationException);
            }

            if (modelId != null && MlStrings.isValidId(modelId) == false) {
                validationException = addValidationError(Messages.getMessage(Messages.INVALID_ID,
                    TrainedModelConfig.MODEL_ID.getPreferredName(),
                    modelId),
                    validationException);
            }
            if (modelId != null && MlStrings.hasValidLengthForId(modelId) == false) {
                validationException = addValidationError(Messages.getMessage(Messages.ID_TOO_LONG,
                    TrainedModelConfig.MODEL_ID.getPreferredName(),
                    modelId,
                    MlStrings.ID_LENGTH_LIMIT), validationException);
            }
            List<String> badTags = tags.stream()
                .filter(tag -> (MlStrings.isValidId(tag) && MlStrings.hasValidLengthForId(tag)) == false)
                .collect(Collectors.toList());
            if (badTags.isEmpty() == false) {
                validationException = addValidationError(Messages.getMessage(Messages.INFERENCE_INVALID_TAGS,
                    badTags,
                    MlStrings.ID_LENGTH_LIMIT),
                    validationException);
            }

            for(String tag : tags) {
                if (tag.equals(modelId)) {
                    validationException = addValidationError("none of the tags must equal the model_id", validationException);
                    break;
                }
            }
            if (input != null && input.getFieldNames().isEmpty()) {
                validationException = addValidationError("[input.field_names] must not be empty", validationException);
            }
            if (input != null && input.getFieldNames()
                .stream()
                .filter(s -> s.contains("."))
                .flatMap(s -> Arrays.stream(Strings.delimitedListToStringArray(s, ".")))
                .anyMatch(String::isEmpty)) {
                validationException = addValidationError("[input.field_names] must only contain valid dot delimited field names",
                    validationException);
            }
            if (forCreation) {
                validationException = checkIllegalSetting(version, VERSION.getPreferredName(), validationException);
                validationException = checkIllegalSetting(createdBy, CREATED_BY.getPreferredName(), validationException);
                validationException = checkIllegalSetting(createTime, CREATE_TIME.getPreferredName(), validationException);
                validationException = checkIllegalSetting(estimatedHeapMemory,
                    ESTIMATED_HEAP_MEMORY_USAGE_BYTES.getPreferredName(),
                    validationException);
                validationException = checkIllegalSetting(estimatedOperations,
                    ESTIMATED_OPERATIONS.getPreferredName(),
                    validationException);
                validationException = checkIllegalSetting(licenseLevel, LICENSE_LEVEL.getPreferredName(), validationException);
                if (metadata != null) {
                    validationException = checkIllegalSetting(
                        metadata.get(TOTAL_FEATURE_IMPORTANCE),
                        METADATA.getPreferredName() + "." + TOTAL_FEATURE_IMPORTANCE,
                        validationException);
                    validationException = checkIllegalSetting(
                        metadata.get(MODEL_ALIASES),
                        METADATA.getPreferredName() + "." + MODEL_ALIASES,
                        validationException);
                }
            }

            if (validationException != null) {
                throw validationException;
            }

            return this;
        }

        private static ActionRequestValidationException checkIllegalSetting(Object value,
                                                                            String setting,
                                                                            ActionRequestValidationException validationException) {
            if (value != null) {
                return addValidationError("illegal to set [" + setting + "] at inference model creation", validationException);
            }
            return validationException;
        }

        public TrainedModelConfig build() {
            return new TrainedModelConfig(
                modelId,
                modelType,
                createdBy == null ? "user" : createdBy,
                version == null ? Version.CURRENT : version,
                description,
                createTime == null ? Instant.now() : createTime,
                definition,
                tags,
                metadata,
                input,
                estimatedHeapMemory == null ? 0 : estimatedHeapMemory,
                estimatedOperations == null ? 0 : estimatedOperations,
                licenseLevel == null ? License.OperationMode.PLATINUM.description() : licenseLevel,
                defaultFieldMap,
                inferenceConfig,
                location);
        }
    }

    static class LazyModelDefinition implements ToXContentObject, Writeable {

        private BytesReference compressedRepresentation;
        private TrainedModelDefinition parsedDefinition;

        public static LazyModelDefinition fromParsedDefinition(TrainedModelDefinition definition) {
            return new LazyModelDefinition(null, definition);
        }

        public static LazyModelDefinition fromCompressedData(BytesReference compressed) {
            return new LazyModelDefinition(compressed, null);
        }

        public static LazyModelDefinition fromBase64String(String base64String) {
            byte[] decodedBytes = Base64.getDecoder().decode(base64String);
            return new LazyModelDefinition(new BytesArray(decodedBytes), null);
        }

        public static LazyModelDefinition fromStreamInput(StreamInput input) throws IOException {
            if (input.getVersion().onOrAfter(Version.V_8_0_0)) { // TODO adjust on backport
                return new LazyModelDefinition(input.readBytesReference(), null);
            } else {
                return fromBase64String(input.readString());
            }
        }

        private LazyModelDefinition(LazyModelDefinition definition) {
            if (definition != null) {
                this.compressedRepresentation = definition.compressedRepresentation;
                this.parsedDefinition = definition.parsedDefinition;
            }
        }

        private LazyModelDefinition(BytesReference compressedRepresentation, TrainedModelDefinition trainedModelDefinition) {
            if (compressedRepresentation == null && trainedModelDefinition == null) {
                throw new IllegalArgumentException("unexpected null model definition");
            }
            this.compressedRepresentation = compressedRepresentation;
            this.parsedDefinition = trainedModelDefinition;
        }

        private BytesReference getCompressedDefinition() throws IOException {
            if (compressedRepresentation == null) {
                compressedRepresentation = InferenceToXContentCompressor.deflate(parsedDefinition);
            }
            return compressedRepresentation;
        }

        private String getBase64CompressedDefinition() throws IOException {
            BytesReference compressedDef = getCompressedDefinition();

            ByteBuffer bb = Base64.getEncoder().encode(
                ByteBuffer.wrap(compressedDef.array(), compressedDef.arrayOffset(), compressedDef.length()));

            return new String(bb.array(), StandardCharsets.UTF_8);
        }

        private void ensureParsedDefinition(NamedXContentRegistry xContentRegistry) throws IOException {
            if (parsedDefinition == null) {
                parsedDefinition = InferenceToXContentCompressor.inflate(compressedRepresentation,
                    parser -> TrainedModelDefinition.fromXContent(parser, true).build(),
                    xContentRegistry);
            }
        }

        private void ensureParsedDefinitionUnsafe(NamedXContentRegistry xContentRegistry) throws IOException {
            if (parsedDefinition == null) {
                parsedDefinition = InferenceToXContentCompressor.inflateUnsafe(compressedRepresentation,
                    parser -> TrainedModelDefinition.fromXContent(parser, true).build(),
                    xContentRegistry);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getVersion().onOrAfter(Version.V_8_0_0)) { // TODO adjust on backport
                out.writeBytesReference(getCompressedDefinition());
            } else {
                out.writeString(getBase64CompressedDefinition());
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (parsedDefinition != null) {
                return parsedDefinition.toXContent(builder, params);
            }
            Map<String, Object> map = InferenceToXContentCompressor.inflateToMap(compressedRepresentation);
            return builder.map(map);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LazyModelDefinition that = (LazyModelDefinition) o;
            return Objects.equals(compressedRepresentation, that.compressedRepresentation) &&
                Objects.equals(parsedDefinition, that.parsedDefinition);
        }

        @Override
        public int hashCode() {
            return Objects.hash(compressedRepresentation, parsedDefinition);
        }
    }
}
