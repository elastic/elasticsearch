/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.license.License;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.common.time.TimeUtils;
import org.elasticsearch.xpack.core.ml.MlConfigVersion;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LenientlyParsedInferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LenientlyParsedTrainedModelLocation;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ModelPackageConfig;
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
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.core.Strings.format;
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
    public static final String DEFINITION_STATUS = "definition_status";

    private static final String ESTIMATED_HEAP_MEMORY_USAGE_HUMAN = "estimated_heap_memory_usage";
    private static final String MODEL_SIZE_HUMAN = "model_size";

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
    public static final ParseField MODEL_SIZE_BYTES = new ParseField("model_size_bytes");
    public static final ParseField MODEL_SIZE_BYTES_WITH_DEPRECATION = new ParseField(
        "model_size_bytes",
        "estimated_heap_memory_usage_bytes"
    );
    public static final ParseField DEPRECATED_ESTIMATED_HEAP_MEMORY_USAGE_BYTES = new ParseField("estimated_heap_memory_usage_bytes");
    public static final ParseField ESTIMATED_OPERATIONS = new ParseField("estimated_operations");
    public static final ParseField LICENSE_LEVEL = new ParseField("license_level");
    public static final ParseField DEFAULT_FIELD_MAP = new ParseField("default_field_map");
    public static final ParseField INFERENCE_CONFIG = new ParseField("inference_config");
    public static final ParseField LOCATION = new ParseField("location");
    public static final ParseField MODEL_PACKAGE = new ParseField("model_package");
    public static final ParseField PREFIX_STRINGS = new ParseField("prefix_strings");

    public static final ParseField PER_DEPLOYMENT_MEMORY_BYTES = new ParseField("per_deployment_memory_bytes");
    public static final ParseField PER_ALLOCATION_MEMORY_BYTES = new ParseField("per_allocation_memory_bytes");
    public static final ParseField PLATFORM_ARCHITECTURE = new ParseField("platform_architecture");

    public static final TransportVersion VERSION_3RD_PARTY_CONFIG_ADDED = TransportVersions.V_8_0_0;
    public static final TransportVersion VERSION_ALLOCATION_MEMORY_ADDED = TransportVersions.V_8_11_X;

    // These parsers follow the pattern that metadata is parsed leniently (to allow for enhancements), whilst config is parsed strictly
    public static final ObjectParser<TrainedModelConfig.Builder, Void> LENIENT_PARSER = createParser(true);
    public static final ObjectParser<TrainedModelConfig.Builder, Void> STRICT_PARSER = createParser(false);

    private static ObjectParser<TrainedModelConfig.Builder, Void> createParser(boolean ignoreUnknownFields) {
        ObjectParser<TrainedModelConfig.Builder, Void> parser = new ObjectParser<>(
            NAME,
            ignoreUnknownFields,
            TrainedModelConfig.Builder::new
        );
        parser.declareString(TrainedModelConfig.Builder::setModelId, MODEL_ID);
        parser.declareString(TrainedModelConfig.Builder::setModelType, MODEL_TYPE);
        parser.declareString(TrainedModelConfig.Builder::setCreatedBy, CREATED_BY);
        parser.declareString(TrainedModelConfig.Builder::setVersion, VERSION);
        parser.declareString(TrainedModelConfig.Builder::setDescription, DESCRIPTION);
        parser.declareField(
            TrainedModelConfig.Builder::setCreateTime,
            (p, c) -> TimeUtils.parseTimeFieldToInstant(p, CREATE_TIME.getPreferredName()),
            CREATE_TIME,
            ObjectParser.ValueType.VALUE
        );
        parser.declareStringArray(TrainedModelConfig.Builder::setTags, TAGS);
        parser.declareObject(TrainedModelConfig.Builder::setMetadata, (p, c) -> p.map(), METADATA);
        parser.declareString((trainedModelConfig, s) -> {}, InferenceIndexConstants.DOC_TYPE);
        parser.declareObject(TrainedModelConfig.Builder::setInput, (p, c) -> TrainedModelInput.fromXContent(p, ignoreUnknownFields), INPUT);
        if (ignoreUnknownFields) {
            // On reading from the index, we automatically translate to the new field, no need have a deprecation warning
            parser.declareLong(TrainedModelConfig.Builder::setModelSize, DEPRECATED_ESTIMATED_HEAP_MEMORY_USAGE_BYTES);
            parser.declareLong(TrainedModelConfig.Builder::setModelSize, MODEL_SIZE_BYTES);
        } else {
            // If this is a new PUT, we should indicate that `estimated_heap_memory_usage_bytes` is deprecated
            parser.declareLong(TrainedModelConfig.Builder::setModelSize, MODEL_SIZE_BYTES_WITH_DEPRECATION);
        }
        parser.declareLong(TrainedModelConfig.Builder::setEstimatedOperations, ESTIMATED_OPERATIONS);
        parser.declareObject(
            TrainedModelConfig.Builder::setLazyDefinition,
            (p, c) -> TrainedModelDefinition.fromXContent(p, ignoreUnknownFields),
            DEFINITION
        );
        parser.declareString(TrainedModelConfig.Builder::setLazyDefinition, COMPRESSED_DEFINITION);
        parser.declareString(TrainedModelConfig.Builder::setLicenseLevel, LICENSE_LEVEL);
        parser.declareObject(TrainedModelConfig.Builder::setDefaultFieldMap, (p, c) -> p.mapStrings(), DEFAULT_FIELD_MAP);
        parser.declareNamedObject(
            TrainedModelConfig.Builder::setInferenceConfig,
            (p, c, n) -> ignoreUnknownFields
                ? p.namedObject(LenientlyParsedInferenceConfig.class, n, null)
                : p.namedObject(StrictlyParsedInferenceConfig.class, n, null),
            INFERENCE_CONFIG
        );
        parser.declareNamedObject(
            TrainedModelConfig.Builder::setLocation,
            (p, c, n) -> ignoreUnknownFields
                ? p.namedObject(LenientlyParsedTrainedModelLocation.class, n, null)
                : p.namedObject(StrictlyParsedTrainedModelLocation.class, n, null),
            LOCATION
        );
        parser.declareObject(
            TrainedModelConfig.Builder::setModelPackageConfig,
            (p, c) -> ignoreUnknownFields ? ModelPackageConfig.fromXContentLenient(p) : ModelPackageConfig.fromXContentStrict(p),
            MODEL_PACKAGE
        );
        parser.declareString(TrainedModelConfig.Builder::setPlatformArchitecture, PLATFORM_ARCHITECTURE);
        parser.declareObject(
            TrainedModelConfig.Builder::setPrefixStrings,
            (p, c) -> TrainedModelPrefixStrings.fromXContent(p, ignoreUnknownFields),
            PREFIX_STRINGS
        );

        return parser;
    }

    public static TrainedModelConfig.Builder fromXContent(XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.parse(parser, null) : STRICT_PARSER.parse(parser, null);
    }

    private final String modelId;
    private final String createdBy;
    private final MlConfigVersion version;
    private final String description;
    private final Instant createTime;
    private final TrainedModelType modelType;
    private final List<String> tags;
    private final Map<String, Object> metadata;
    private final TrainedModelInput input;
    private final long modelSize;
    private final long estimatedOperations;
    private final License.OperationMode licenseLevel;
    private final Map<String, String> defaultFieldMap;
    private final InferenceConfig inferenceConfig;

    private final LazyModelDefinition definition;
    private final TrainedModelLocation location;
    private final ModelPackageConfig modelPackageConfig;
    private Boolean fullDefinition;
    private String platformArchitecture;
    private TrainedModelPrefixStrings prefixStrings;

    TrainedModelConfig(
        String modelId,
        TrainedModelType modelType,
        String createdBy,
        MlConfigVersion version,
        String description,
        Instant createTime,
        LazyModelDefinition definition,
        List<String> tags,
        Map<String, Object> metadata,
        TrainedModelInput input,
        Long modelSize,
        Long estimatedOperations,
        String licenseLevel,
        Map<String, String> defaultFieldMap,
        InferenceConfig inferenceConfig,
        TrainedModelLocation location,
        ModelPackageConfig modelPackageConfig,
        String platformArchitecture,
        TrainedModelPrefixStrings prefixStrings
    ) {
        this.modelId = ExceptionsHelper.requireNonNull(modelId, MODEL_ID);
        this.modelType = modelType;
        this.createdBy = ExceptionsHelper.requireNonNull(createdBy, CREATED_BY);
        this.version = ExceptionsHelper.requireNonNull(version, VERSION);
        this.createTime = Instant.ofEpochMilli(ExceptionsHelper.requireNonNull(createTime, CREATE_TIME).toEpochMilli());
        this.definition = definition;
        this.description = description;
        this.tags = Collections.unmodifiableList(ExceptionsHelper.requireNonNull(tags, TAGS));
        this.metadata = metadata == null ? null : Collections.unmodifiableMap(metadata);
        this.input = ExceptionsHelper.requireNonNull(handleDefaultInput(input, inferenceConfig, modelType), INPUT);
        if (ExceptionsHelper.requireNonNull(modelSize, MODEL_SIZE_BYTES) < 0) {
            throw new IllegalArgumentException("[" + MODEL_SIZE_BYTES.getPreferredName() + "] must be greater than or equal to 0");
        }
        this.modelSize = modelSize;
        if (ExceptionsHelper.requireNonNull(estimatedOperations, ESTIMATED_OPERATIONS) < 0) {
            throw new IllegalArgumentException("[" + ESTIMATED_OPERATIONS.getPreferredName() + "] must be greater than or equal to 0");
        }
        this.estimatedOperations = estimatedOperations;
        this.licenseLevel = License.OperationMode.parse(ExceptionsHelper.requireNonNull(licenseLevel, LICENSE_LEVEL));
        assert this.licenseLevel.equals(License.OperationMode.PLATINUM) || this.licenseLevel.equals(License.OperationMode.BASIC)
            : "[" + LICENSE_LEVEL.getPreferredName() + "] only [platinum] or [basic] is supported";
        this.defaultFieldMap = defaultFieldMap == null ? null : Collections.unmodifiableMap(defaultFieldMap);
        this.inferenceConfig = inferenceConfig;
        this.location = location;
        this.modelPackageConfig = modelPackageConfig;
        this.platformArchitecture = platformArchitecture;
        this.prefixStrings = prefixStrings;
    }

    private static TrainedModelInput handleDefaultInput(
        TrainedModelInput input,
        InferenceConfig inferenceConfig,
        TrainedModelType modelType
    ) {
        return input == null && inferenceConfig != null ? inferenceConfig.getDefaultInput(modelType) : input;
    }

    public TrainedModelConfig(StreamInput in) throws IOException {
        modelId = in.readString();
        createdBy = in.readString();
        version = MlConfigVersion.readVersion(in);
        description = in.readOptionalString();
        createTime = in.readInstant();
        definition = in.readOptionalWriteable(LazyModelDefinition::fromStreamInput);
        tags = in.readCollectionAsImmutableList(StreamInput::readString);
        metadata = in.readGenericMap();
        input = new TrainedModelInput(in);
        modelSize = in.readVLong();
        estimatedOperations = in.readVLong();
        licenseLevel = License.OperationMode.parse(in.readString());
        this.defaultFieldMap = in.readBoolean() ? in.readImmutableMap(StreamInput::readString) : null;

        this.inferenceConfig = in.readOptionalNamedWriteable(InferenceConfig.class);
        if (in.getTransportVersion().onOrAfter(VERSION_3RD_PARTY_CONFIG_ADDED)) {
            this.modelType = in.readOptionalEnum(TrainedModelType.class);
            this.location = in.readOptionalNamedWriteable(TrainedModelLocation.class);
        } else {
            this.modelType = null;
            this.location = null;
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0)) {
            modelPackageConfig = in.readOptionalWriteable(ModelPackageConfig::new);
            fullDefinition = in.readOptionalBoolean();
        } else {
            modelPackageConfig = null;
            fullDefinition = null;
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_11_X)) {
            platformArchitecture = in.readOptionalString();
        } else {
            platformArchitecture = null;
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            prefixStrings = in.readOptionalWriteable(TrainedModelPrefixStrings::new);
        }
    }

    public boolean isPackagedModel() {
        return modelId.startsWith(".");
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

    public MlConfigVersion getVersion() {
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

    public BytesReference getCompressedDefinitionIfSet() {
        if (definition == null) {
            return null;
        }
        return definition.getCompressedDefinitionIfSet();
    }

    public ModelPackageConfig getModelPackageConfig() {
        return modelPackageConfig;
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

    public long getModelSize() {
        return modelSize;
    }

    public long getEstimatedOperations() {
        return estimatedOperations;
    }

    // TODO if we ever support anything other than "basic" and platinum, we need to adjust our feature tracking logic
    // and we need to adjust our license checks to validate more than "is basic" or not
    public License.OperationMode getLicenseLevel() {
        return licenseLevel;
    }

    public boolean isAllocateOnly() {
        return Optional.ofNullable(inferenceConfig).map(InferenceConfig::isAllocateOnly).orElse(false);
    }

    public void setFullDefinition(boolean fullDefinition) {
        this.fullDefinition = fullDefinition;
    }

    public long getPerDeploymentMemoryBytes() {
        return metadata != null && metadata.containsKey(PER_DEPLOYMENT_MEMORY_BYTES.getPreferredName())
            ? ((Number) metadata.get(PER_DEPLOYMENT_MEMORY_BYTES.getPreferredName())).longValue()
            : 0L;
    }

    public long getPerAllocationMemoryBytes() {
        return metadata != null && metadata.containsKey(PER_ALLOCATION_MEMORY_BYTES.getPreferredName())
            ? ((Number) metadata.get(PER_ALLOCATION_MEMORY_BYTES.getPreferredName())).longValue()
            : 0L;
    }

    public String getPlatformArchitecture() {
        return platformArchitecture;
    }

    public TrainedModelPrefixStrings getPrefixStrings() {
        return prefixStrings;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        out.writeString(createdBy);
        MlConfigVersion.writeVersion(version, out);
        out.writeOptionalString(description);
        out.writeInstant(createTime);
        out.writeOptionalWriteable(definition);
        out.writeStringCollection(tags);
        out.writeGenericMap(metadata);
        input.writeTo(out);
        out.writeVLong(modelSize);
        out.writeVLong(estimatedOperations);
        out.writeString(licenseLevel.description());
        if (defaultFieldMap != null) {
            out.writeBoolean(true);
            out.writeMap(defaultFieldMap, StreamOutput::writeString);
        } else {
            out.writeBoolean(false);
        }
        out.writeOptionalNamedWriteable(inferenceConfig);
        if (out.getTransportVersion().onOrAfter(VERSION_3RD_PARTY_CONFIG_ADDED)) {
            out.writeOptionalEnum(modelType);
            out.writeOptionalNamedWriteable(location);
        }

        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0)) {
            out.writeOptionalWriteable(modelPackageConfig);
            out.writeOptionalBoolean(fullDefinition);
        }

        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_11_X)) {
            out.writeOptionalString(platformArchitecture);
        }

        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            out.writeOptionalWriteable(prefixStrings);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MODEL_ID.getPreferredName(), modelId);
        if (modelType != null) {
            builder.field(MODEL_TYPE.getPreferredName(), modelType.toString());
        }
        if (modelPackageConfig != null) {
            builder.field(MODEL_PACKAGE.getPreferredName(), modelPackageConfig);
        }
        if (platformArchitecture != null) {
            builder.field(PLATFORM_ARCHITECTURE.getPreferredName(), platformArchitecture);
        }

        // If the model is to be exported for future import to another cluster, these fields are irrelevant.
        if (params.paramAsBoolean(EXCLUDE_GENERATED, false) == false) {
            builder.field(CREATED_BY.getPreferredName(), createdBy);
            builder.field(VERSION.getPreferredName(), version.toString());
            builder.timestampFieldsFromUnixEpochMillis(
                CREATE_TIME.getPreferredName(),
                CREATE_TIME.getPreferredName() + "_string",
                createTime.toEpochMilli()
            );
            builder.humanReadableField(MODEL_SIZE_BYTES.getPreferredName(), MODEL_SIZE_HUMAN, ByteSizeValue.ofBytes(modelSize));
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
        if (prefixStrings != null) {
            builder.field(PREFIX_STRINGS.getPreferredName(), prefixStrings);
        }
        if (params.paramAsBoolean(DEFINITION_STATUS, false) && fullDefinition != null) {
            builder.field("fully_defined", fullDefinition);
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
        return Objects.equals(modelId, that.modelId)
            && Objects.equals(modelType, that.modelType)
            && Objects.equals(modelPackageConfig, that.modelPackageConfig)
            && Objects.equals(createdBy, that.createdBy)
            && Objects.equals(version, that.version)
            && Objects.equals(description, that.description)
            && Objects.equals(createTime, that.createTime)
            && Objects.equals(definition, that.definition)
            && Objects.equals(tags, that.tags)
            && Objects.equals(input, that.input)
            && Objects.equals(modelSize, that.modelSize)
            && Objects.equals(estimatedOperations, that.estimatedOperations)
            && Objects.equals(licenseLevel, that.licenseLevel)
            && Objects.equals(defaultFieldMap, that.defaultFieldMap)
            && Objects.equals(inferenceConfig, that.inferenceConfig)
            && Objects.equals(metadata, that.metadata)
            && Objects.equals(location, that.location)
            && Objects.equals(platformArchitecture, that.platformArchitecture)
            && Objects.equals(prefixStrings, that.prefixStrings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            modelId,
            modelType,
            modelPackageConfig,
            createdBy,
            version,
            createTime,
            definition,
            description,
            tags,
            metadata,
            modelSize,
            estimatedOperations,
            input,
            licenseLevel,
            inferenceConfig,
            defaultFieldMap,
            location,
            platformArchitecture,
            prefixStrings
        );
    }

    public static class Builder {

        private String modelId;
        private TrainedModelType modelType;
        private String createdBy;
        private MlConfigVersion version;
        private String description;
        private Instant createTime;
        private List<String> tags = Collections.emptyList();
        private Map<String, Object> metadata;
        private TrainedModelInput input;
        private Long modelSize;
        private Long estimatedOperations;
        private LazyModelDefinition definition;
        private String licenseLevel;
        private Map<String, String> defaultFieldMap;
        private InferenceConfig inferenceConfig;
        private TrainedModelLocation location;
        private ModelPackageConfig modelPackageConfig;
        private String platformArchitecture;
        private TrainedModelPrefixStrings prefixStrings;

        public Builder() {}

        public Builder(TrainedModelConfig config) {
            this.modelId = config.getModelId();
            this.modelType = config.modelType;
            this.createdBy = config.getCreatedBy();
            this.version = config.getVersion();
            this.createTime = config.getCreateTime();
            this.definition = config.definition == null ? null : new LazyModelDefinition(config.definition);
            this.description = config.getDescription();
            this.tags = config.getTags();
            this.metadata = config.getMetadata() == null ? null : new HashMap<>(config.getMetadata());
            this.input = config.getInput();
            this.estimatedOperations = config.estimatedOperations;
            this.modelSize = config.modelSize;
            this.licenseLevel = config.licenseLevel.description();
            this.defaultFieldMap = config.defaultFieldMap == null ? null : new HashMap<>(config.defaultFieldMap);
            this.inferenceConfig = config.inferenceConfig;
            this.location = config.location;
            this.modelPackageConfig = config.modelPackageConfig;
            this.platformArchitecture = config.platformArchitecture;
            this.prefixStrings = config.prefixStrings;
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

        public Builder setModelPackageConfig(ModelPackageConfig modelPackageConfig) {
            this.modelPackageConfig = modelPackageConfig;
            return this;
        }

        public Builder setCreatedBy(String createdBy) {
            this.createdBy = createdBy;
            return this;
        }

        public MlConfigVersion getVersion() {
            return version;
        }

        public Builder setVersion(MlConfigVersion version) {
            this.version = version;
            return this;
        }

        private Builder setVersion(String version) {
            return this.setVersion(MlConfigVersion.fromString(version));
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
            return addToMetadata(HYPERPARAMETERS, hyperparameters.stream().map(Hyperparameters::asMap).collect(Collectors.toList()));
        }

        public Builder setPlatformArchitecture(String platformArchitecture) {
            this.platformArchitecture = platformArchitecture;
            return this;
        }

        public Builder setPrefixStrings(TrainedModelPrefixStrings prefixStrings) {
            this.prefixStrings = prefixStrings;
            return this;
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

        public Builder setParsedDefinition(TrainedModelDefinition.Builder definitionRef) {
            if (definitionRef == null) {
                return this;
            }
            this.definition = LazyModelDefinition.fromParsedDefinition(definitionRef.build());
            return this;
        }

        public Builder setDefinitionFromBytes(BytesReference definitionRef) {
            if (definitionRef == null) {
                return this;
            }
            this.definition = LazyModelDefinition.fromCompressedData(definitionRef);
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
                throw new IllegalArgumentException(
                    format("both [%s] and [%s] cannot be set.", COMPRESSED_DEFINITION.getPreferredName(), DEFINITION.getPreferredName())
                );
            }
            this.definition = LazyModelDefinition.fromParsedDefinition(parsedTrainedModel.build());
            return this;
        }

        private Builder setLazyDefinition(String compressedString) {
            if (compressedString == null) {
                return this;
            }

            if (this.definition != null) {
                throw new IllegalArgumentException(
                    format("both [%s] and [%s] cannot be set.", COMPRESSED_DEFINITION.getPreferredName(), DEFINITION.getPreferredName())
                );
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

        public Builder setModelSize(long modelSize) {
            this.modelSize = modelSize;
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
         * @param forCreation indicates if we should validate for model creation or for a model read from storage
         * @return The current builder object if validations are successful
         * @throws ActionRequestValidationException when there are validation failures.
         */
        public Builder validate(boolean forCreation) {
            // We require a definition to be available here even though it will be stored in a different doc
            ActionRequestValidationException validationException = null;
            boolean packagedModel = modelId != null && modelId.startsWith(".");

            if (modelId == null) {
                validationException = addValidationError("[" + MODEL_ID.getPreferredName() + "] must not be null.", validationException);
            }

            if (packagedModel == false) {
                if (definition != null && location != null) {
                    validationException = addValidationError(
                        "["
                            + DEFINITION.getPreferredName()
                            + "] "
                            + "and ["
                            + LOCATION.getPreferredName()
                            + "] are both defined but only one can be used.",
                        validationException
                    );
                }
                if (definition == null && modelType == null) {
                    validationException = addValidationError(
                        "["
                            + MODEL_TYPE.getPreferredName()
                            + "] must be set if "
                            + "["
                            + DEFINITION.getPreferredName()
                            + "] is not defined.",
                        validationException
                    );
                }

                if (inferenceConfig == null && forCreation) {
                    validationException = addValidationError(
                        "[" + INFERENCE_CONFIG.getPreferredName() + "] must not be null.",
                        validationException
                    );
                }
            }

            if (modelId != null) {
                if (packagedModel) {
                    String idToValidate = modelId.substring(1);
                    if (idToValidate.endsWith("_SNAPSHOT")) {
                        idToValidate = idToValidate.substring(0, idToValidate.length() - 9);
                    }

                    if (MlStrings.isValidId(idToValidate) == false) {
                        validationException = addValidationError(
                            Messages.getMessage(Messages.INVALID_MODEL_PACKAGE_ID, TrainedModelConfig.MODEL_ID.getPreferredName(), modelId),
                            validationException
                        );
                    }
                } else {
                    if (MlStrings.isValidId(modelId) == false) {
                        validationException = addValidationError(
                            Messages.getMessage(Messages.INVALID_ID, TrainedModelConfig.MODEL_ID.getPreferredName(), modelId),
                            validationException
                        );
                    }
                }
            }

            if (modelId != null && MlStrings.hasValidLengthForId(modelId) == false) {
                validationException = addValidationError(
                    Messages.getMessage(
                        Messages.ID_TOO_LONG,
                        TrainedModelConfig.MODEL_ID.getPreferredName(),
                        modelId,
                        MlStrings.ID_LENGTH_LIMIT
                    ),
                    validationException
                );
            }
            List<String> badTags = tags.stream()
                .filter(tag -> (MlStrings.isValidId(tag) && MlStrings.hasValidLengthForId(tag)) == false)
                .collect(Collectors.toList());
            if (badTags.isEmpty() == false) {
                validationException = addValidationError(
                    Messages.getMessage(Messages.INFERENCE_INVALID_TAGS, badTags, MlStrings.ID_LENGTH_LIMIT),
                    validationException
                );
            }

            for (String tag : tags) {
                if (tag.equals(modelId)) {
                    validationException = addValidationError("none of the tags must equal the model_id", validationException);
                    break;
                }
            }

            // Delegate input validation to the inference config.
            if (inferenceConfig != null) {
                validationException = inferenceConfig.validateTrainedModelInput(input, forCreation, validationException);
            }

            if (forCreation) {
                validationException = checkIllegalSetting(version, VERSION.getPreferredName(), validationException);
                validationException = checkIllegalSetting(createdBy, CREATED_BY.getPreferredName(), validationException);
                validationException = checkIllegalSetting(createTime, CREATE_TIME.getPreferredName(), validationException);
                validationException = checkIllegalSetting(licenseLevel, LICENSE_LEVEL.getPreferredName(), validationException);
                validationException = checkIllegalSetting(location, LOCATION.getPreferredName(), validationException);
                if (metadata != null) {
                    validationException = checkIllegalSetting(
                        metadata.get(TOTAL_FEATURE_IMPORTANCE),
                        METADATA.getPreferredName() + "." + TOTAL_FEATURE_IMPORTANCE,
                        validationException
                    );
                    validationException = checkIllegalSetting(
                        metadata.get(MODEL_ALIASES),
                        METADATA.getPreferredName() + "." + MODEL_ALIASES,
                        validationException
                    );
                }

                // packaged model validation
                validationException = checkIllegalSetting(modelPackageConfig, MODEL_PACKAGE.getPreferredName(), validationException);
            }
            if (validationException != null) {
                throw validationException;
            }

            return this;
        }

        /**
         * Validate that fields defined by the package aren't defined in the request.
         *
         * To be called by the transport after checking that the package exists.
         */
        public Builder validateNoPackageOverrides() {
            ActionRequestValidationException validationException = null;
            validationException = checkIllegalPackagedModelSetting(description, DESCRIPTION.getPreferredName(), validationException);
            validationException = checkIllegalPackagedModelSetting(definition, DEFINITION.getPreferredName(), validationException);
            validationException = checkIllegalPackagedModelSetting(modelType, MODEL_TYPE.getPreferredName(), validationException);
            validationException = checkIllegalPackagedModelSetting(metadata, METADATA.getPreferredName(), validationException);
            if (modelSize != null && modelSize > 0) {
                validationException = checkIllegalPackagedModelSetting(modelSize, MODEL_SIZE_BYTES.getPreferredName(), validationException);
            }
            validationException = checkIllegalPackagedModelSetting(
                inferenceConfig,
                INFERENCE_CONFIG.getPreferredName(),
                validationException
            );
            if (tags != null && tags.isEmpty() == false) {
                validationException = addValidationError(
                    "illegal to set [tags] at inference model creation for packaged model",
                    validationException
                );
            }

            if (validationException != null) {
                throw validationException;
            }

            return this;
        }

        private static ActionRequestValidationException checkIllegalSetting(
            Object value,
            String setting,
            ActionRequestValidationException validationException
        ) {
            if (value != null) {
                return addValidationError("illegal to set [" + setting + "] at inference model creation", validationException);
            }
            return validationException;
        }

        private static ActionRequestValidationException checkIllegalPackagedModelSetting(
            Object value,
            String setting,
            ActionRequestValidationException validationException
        ) {
            if (value != null) {
                return addValidationError(
                    "illegal to set [" + setting + "] at inference model creation for packaged model",
                    validationException
                );
            }
            return validationException;
        }

        public TrainedModelConfig build() {
            return new TrainedModelConfig(
                modelId,
                modelType,
                createdBy == null ? "user" : createdBy,
                version == null ? MlConfigVersion.CURRENT : version,
                description,
                createTime == null ? Instant.now() : createTime,
                definition,
                tags,
                metadata,
                input,
                modelSize == null ? 0 : modelSize,
                estimatedOperations == null ? 0 : estimatedOperations,
                licenseLevel == null ? License.OperationMode.PLATINUM.description() : licenseLevel,
                defaultFieldMap,
                inferenceConfig,
                location,
                modelPackageConfig,
                platformArchitecture,
                prefixStrings
            );
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
            if (input.getTransportVersion().onOrAfter(TransportVersions.V_8_0_0)) {
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

        private BytesReference getCompressedDefinitionIfSet() {
            return compressedRepresentation;
        }

        private String getBase64CompressedDefinition() throws IOException {
            BytesReference compressedDef = getCompressedDefinition();

            ByteBuffer bb = Base64.getEncoder()
                .encode(ByteBuffer.wrap(compressedDef.array(), compressedDef.arrayOffset(), compressedDef.length()));

            return new String(bb.array(), StandardCharsets.UTF_8);
        }

        private void ensureParsedDefinition(NamedXContentRegistry xContentRegistry) throws IOException {
            if (parsedDefinition == null) {
                parsedDefinition = InferenceToXContentCompressor.inflate(
                    compressedRepresentation,
                    parser -> TrainedModelDefinition.fromXContent(parser, true).build(),
                    xContentRegistry
                );
            }
        }

        private void ensureParsedDefinitionUnsafe(NamedXContentRegistry xContentRegistry) throws IOException {
            if (parsedDefinition == null) {
                parsedDefinition = InferenceToXContentCompressor.inflateUnsafe(
                    compressedRepresentation,
                    parser -> TrainedModelDefinition.fromXContent(parser, true).build(),
                    xContentRegistry
                );
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_0_0)) {
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
            return Objects.equals(compressedRepresentation, that.compressedRepresentation)
                && Objects.equals(parsedDefinition, that.parsedDefinition);
        }

        @Override
        public int hashCode() {
            return Objects.hash(compressedRepresentation, parsedDefinition);
        }
    }
}
