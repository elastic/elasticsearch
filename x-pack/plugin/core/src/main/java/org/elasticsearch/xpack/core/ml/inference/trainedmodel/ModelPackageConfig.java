/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.common.time.TimeUtils;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ModelPackageConfig implements ToXContentObject, Writeable {

    public static final ParseField PACKAGED_MODEL_ID = new ParseField("packaged_model_id");
    public static final ParseField MODEL_REPOSITORY = new ParseField("model_repository");
    public static final ParseField MINIMUM_VERSION = new ParseField("minimum_version");
    public static final ParseField CREATE_TIME = new ParseField("create_time");
    public static final ParseField SIZE = new ParseField("size");
    public static final ParseField CHECKSUM_SHA256 = new ParseField("sha256");
    public static final ParseField VOCABULARY_FILE = new ParseField("vocabulary_file");

    private static final ConstructingObjectParser<ModelPackageConfig, Void> LENIENT_PARSER = createParser(true);
    private static final ConstructingObjectParser<ModelPackageConfig, Void> STRICT_PARSER = createParser(false);

    private static ConstructingObjectParser<ModelPackageConfig, Void> createParser(boolean lenient) {
        ConstructingObjectParser<ModelPackageConfig, Void> parser = new ConstructingObjectParser<>(
            PACKAGED_MODEL_ID.getPreferredName(),
            lenient,
            a -> {
                @SuppressWarnings("unchecked")
                Map<String, Object> inferenceConfigSource = (Map<String, Object>) a[7];
                @SuppressWarnings("unchecked")
                Map<String, Object> metadata = (Map<String, Object>) a[8];
                @SuppressWarnings("unchecked")
                List<String> tags = (List<String>) a[10];

                return new ModelPackageConfig(
                    (String) a[0], // packaged_model_id
                    (String) a[1], // model_repository
                    (String) a[2], // description
                    (String) a[3], // minimum_version
                    (Instant) a[4], // create_time
                    (Long) a[5], // size
                    (String) a[6], // sha256
                    inferenceConfigSource,
                    metadata,
                    (String) a[9], // model_type
                    tags,
                    (String) a[11] // vocabulary file
                );
            }
        );
        parser.declareString(ConstructingObjectParser.constructorArg(), PACKAGED_MODEL_ID);
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), MODEL_REPOSITORY);
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), TrainedModelConfig.DESCRIPTION);
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), MINIMUM_VERSION);
        parser.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> TimeUtils.parseTimeFieldToInstant(p, CREATE_TIME.getPreferredName()),
            CREATE_TIME,
            ObjectParser.ValueType.VALUE
        );
        parser.declareLong(ConstructingObjectParser.optionalConstructorArg(), SIZE);
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), CHECKSUM_SHA256);
        parser.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> p.mapOrdered(),
            TrainedModelConfig.INFERENCE_CONFIG
        );
        parser.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.mapOrdered(), TrainedModelConfig.METADATA);
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), TrainedModelConfig.MODEL_TYPE);
        parser.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), TrainedModelConfig.TAGS);
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), VOCABULARY_FILE);

        return parser;
    }

    public static ModelPackageConfig fromXContentStrict(XContentParser parser) throws IOException {
        return STRICT_PARSER.parse(parser, null);
    }

    public static ModelPackageConfig fromXContentLenient(XContentParser parser) throws IOException {
        return LENIENT_PARSER.parse(parser, null);
    }

    private final String packagedModelId;
    private final String modelRepository;
    private final String description;

    // store the version as a string as it might be a version we don't know about yet
    private final String minimumVersion;
    private final Instant createTime;
    private final long size;
    private final String sha256;
    private final Map<String, Object> inferenceConfigSource;
    private final Map<String, Object> metadata;
    private final String modelType;
    private final List<String> tags;
    private final String vocabularyFile;

    public ModelPackageConfig(
        String packagedModelId,
        String modelRepository,
        String description,
        String minimumVersion,
        Instant createTime,
        Long size,
        String sha256,
        Map<String, Object> inferenceConfigSource,
        Map<String, Object> metadata,
        String modelType,
        List<String> tags,
        String vocabularyFile
    ) {
        this.packagedModelId = ExceptionsHelper.requireNonNull(packagedModelId, PACKAGED_MODEL_ID);
        this.modelRepository = modelRepository;
        this.description = description;
        this.minimumVersion = minimumVersion;
        this.createTime = createTime;
        this.size = size == null ? 0 : size;
        if (this.size < 0) {
            throw new IllegalArgumentException("[size] must not be negative.");
        }
        this.sha256 = sha256;
        this.inferenceConfigSource = inferenceConfigSource;
        this.metadata = metadata == null ? Collections.emptyMap() : Collections.unmodifiableMap(metadata);
        this.modelType = modelType;
        this.tags = tags == null ? Collections.emptyList() : Collections.unmodifiableList(tags);
        this.vocabularyFile = vocabularyFile;
    }

    public ModelPackageConfig(StreamInput in) throws IOException {
        this.packagedModelId = in.readString();
        this.modelRepository = in.readOptionalString();
        this.description = in.readOptionalString();
        this.minimumVersion = in.readOptionalString();
        this.createTime = in.readOptionalInstant();
        this.size = in.readVLong();
        this.sha256 = in.readOptionalString();
        this.inferenceConfigSource = in.readMap();
        this.metadata = in.readMap();
        this.modelType = in.readOptionalString();
        this.tags = in.readOptionalList(StreamInput::readString);
        this.vocabularyFile = in.readOptionalString();
    }

    public String getPackagedModelId() {
        return packagedModelId;
    }

    public String getModelRepository() {
        return modelRepository;
    }

    public String getDescription() {
        return description;
    }

    public String getMinimumVersion() {
        return minimumVersion;
    }

    public Instant getCreateTime() {
        return createTime;
    }

    public long getSize() {
        return size;
    }

    public String getSha256() {
        return sha256;
    }

    public Map<String, Object> getInferenceConfigSource() {
        return inferenceConfigSource;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public String getModelType() {
        return modelType;
    }

    public List<String> getTags() {
        return tags;
    }

    public String getVocabularyFile() {
        return vocabularyFile;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(PACKAGED_MODEL_ID.getPreferredName(), packagedModelId);
        if (Strings.isNullOrEmpty(modelRepository) == false) {
            builder.field(MODEL_REPOSITORY.getPreferredName(), modelRepository);
        }
        if (Strings.isNullOrEmpty(description) == false) {
            builder.field(TrainedModelConfig.DESCRIPTION.getPreferredName(), description);
        }
        if (Strings.isNullOrEmpty(minimumVersion) == false) {
            builder.field(MINIMUM_VERSION.getPreferredName(), minimumVersion);
        }
        if (createTime != null) {
            builder.timeField(CREATE_TIME.getPreferredName(), CREATE_TIME.getPreferredName() + "_string", createTime.toEpochMilli());
        }
        if (size > 0) {
            builder.field(SIZE.getPreferredName(), size);
        }
        if (sha256 != null) {
            builder.field(CHECKSUM_SHA256.getPreferredName(), sha256);
        }
        if (inferenceConfigSource != null) {
            builder.field(TrainedModelConfig.INFERENCE_CONFIG.getPreferredName(), inferenceConfigSource);
        }
        if (metadata != null) {
            builder.field(TrainedModelConfig.METADATA.getPreferredName(), metadata);
        }
        if (Strings.isNullOrEmpty(modelType) == false) {
            builder.field(TrainedModelConfig.MODEL_TYPE.getPreferredName(), modelType);
        }
        if (tags != null) {
            builder.field(TrainedModelConfig.TAGS.getPreferredName(), tags);
        }
        if (Strings.isNullOrEmpty(vocabularyFile) == false) {
            builder.field(VOCABULARY_FILE.getPreferredName(), vocabularyFile);
        }

        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(packagedModelId);
        out.writeOptionalString(modelRepository);
        out.writeOptionalString(description);
        out.writeOptionalString(minimumVersion);
        out.writeOptionalInstant(createTime);
        out.writeVLong(size);
        out.writeOptionalString(sha256);
        out.writeGenericMap(inferenceConfigSource);
        out.writeGenericMap(metadata);
        out.writeOptionalString(modelType);
        out.writeOptionalCollection(tags, StreamOutput::writeString);
        out.writeOptionalString(vocabularyFile);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ModelPackageConfig that = (ModelPackageConfig) o;
        return size == that.size
            && Objects.equals(packagedModelId, that.packagedModelId)
            && Objects.equals(modelRepository, that.modelRepository)
            && Objects.equals(description, that.description)
            && Objects.equals(minimumVersion, that.minimumVersion)
            && Objects.equals(createTime, that.createTime)
            && Objects.equals(sha256, that.sha256)
            && Objects.equals(inferenceConfigSource, that.inferenceConfigSource)
            && Objects.equals(metadata, that.metadata)
            && Objects.equals(modelType, that.modelType)
            && Objects.equals(tags, that.tags)
            && Objects.equals(vocabularyFile, that.vocabularyFile);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            packagedModelId,
            modelRepository,
            description,
            minimumVersion,
            createTime,
            size,
            sha256,
            inferenceConfigSource,
            metadata,
            modelType,
            tags,
            vocabularyFile
        );
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public static class Builder {

        private String packagedModelId;
        private String modelRepository;
        private Instant createTime;
        private String minimumVersion;
        private String description;
        private long size;
        private String sha256;
        private Map<String, Object> inferenceConfigSource;
        private Map<String, Object> metadata;
        private String modelType;
        private List<String> tags;
        private String vocabularyFile;

        public Builder(ModelPackageConfig modelPackageConfig) {
            this.packagedModelId = modelPackageConfig.packagedModelId;
            this.modelRepository = modelPackageConfig.modelRepository;
            this.description = modelPackageConfig.description;
            this.minimumVersion = modelPackageConfig.minimumVersion;
            this.createTime = modelPackageConfig.createTime;
            this.size = modelPackageConfig.size;
            this.sha256 = modelPackageConfig.sha256;
            this.inferenceConfigSource = modelPackageConfig.inferenceConfigSource;
            this.metadata = modelPackageConfig.metadata;
            this.modelType = modelPackageConfig.modelType;
            this.tags = modelPackageConfig.tags;
            this.vocabularyFile = modelPackageConfig.vocabularyFile;
        }

        public Builder setPackedModelId(String packagedModelId) {
            this.packagedModelId = packagedModelId;
            return this;
        }

        public Builder setModelRepository(String modelRepository) {
            this.modelRepository = modelRepository;
            return this;
        }

        public Builder setDescription(String description) {
            this.description = description;
            return this;
        }

        public Builder setMinimumVersion(String minimumVersion) {
            this.minimumVersion = minimumVersion;
            return this;
        }

        public Builder setCreateTime(Instant createTime) {
            this.createTime = createTime;
            return this;
        }

        public Builder setSize(long size) {
            this.size = size;
            return this;
        }

        public Builder setSha256(String sha256) {
            this.sha256 = sha256;
            return this;
        }

        public Builder setInferenceConfigSource(Map<String, Object> inferenceConfigSource) {
            this.inferenceConfigSource = inferenceConfigSource;
            return this;
        }

        public Builder setMetadata(Map<String, Object> metadata) {
            this.metadata = metadata;
            return this;
        }

        public Builder setModelType(String modelType) {
            this.modelType = modelType;
            return this;
        }

        public Builder setTags(List<String> tags) {
            this.tags = tags;
            return this;
        }

        public Builder setVocabularyFile(String vocabularyFile) {
            this.vocabularyFile = vocabularyFile;
            return this;
        }

        /**
         * Reset all fields which are only part of the package metadata, but not be part
         * of the config.
         */
        public Builder resetPackageOnlyFields() {
            this.description = null;
            this.inferenceConfigSource = null;
            this.metadata = null;
            this.modelType = null;
            this.tags = null;
            return this;
        }

        public Builder validate(boolean forCreation) {
            ActionRequestValidationException validationException = null;

            if (validationException != null) {
                throw validationException;
            }
            return this;
        }

        public ModelPackageConfig build() {
            return new ModelPackageConfig(
                packagedModelId,
                modelRepository,
                description,
                minimumVersion,
                createTime,
                size,
                sha256,
                inferenceConfigSource,
                metadata,
                modelType,
                tags,
                vocabularyFile
            );
        }
    }
}
