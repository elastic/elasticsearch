/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;
import org.elasticsearch.xpack.inference.common.parser.StatefulValue;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.inference.InferenceUtils.missingSettingErrorMsg;
import static org.elasticsearch.xpack.inference.common.parser.NumberParser.validatePositiveInteger;
import static org.elasticsearch.xpack.inference.common.parser.StringParser.validateStringIsNotNullOrEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredPositiveInteger;

public class ElasticsearchInternalServiceSettings implements ServiceSettings {

    public static final String NAME = "text_embedding_internal_service_settings";
    private static final int FAILED_INT_PARSE_VALUE = -1;

    public static final String NUM_ALLOCATIONS = "num_allocations";
    public static final String NUM_THREADS = "num_threads";
    public static final String MODEL_ID = "model_id";
    public static final String DEPLOYMENT_ID = "deployment_id";
    public static final String ADAPTIVE_ALLOCATIONS = "adaptive_allocations";

    private static final ObjectParser<Builder, ConfigurationParseContext> REQUEST_PARSER = createParser(false, Builder::new);
    private static final ObjectParser<Builder, ConfigurationParseContext> PERSISTENT_PARSER = createParser(true, Builder::new);

    private Integer numAllocations;
    private final int numThreads;
    private final String modelId;
    private AdaptiveAllocationsSettings adaptiveAllocationsSettings;
    private final String deploymentId;

    /**
     * Creates a parser declaring the settings common to all elasticsearch internal services. Subclasses with additional fields
     * create their parsers through this method with their own builder supplier and declare the extra fields on the result.
     *
     * @param ignoreUnknownFields whether unknown fields are tolerated; {@code false} for user requests, {@code true} for persisted config
     * @param builderSupplier constructs the builder instances the parser populates
     */
    static <B extends Builder> ObjectParser<B, ConfigurationParseContext> createParser(
        boolean ignoreUnknownFields,
        Supplier<B> builderSupplier
    ) {
        ObjectParser<B, ConfigurationParseContext> parser = new ObjectParser<>(
            ModelConfigurations.SERVICE_SETTINGS,
            ignoreUnknownFields,
            builderSupplier
        );
        declareBaseFields(parser);
        return parser;
    }

    private static <B extends Builder> void declareBaseFields(ObjectParser<B, ConfigurationParseContext> parser) {
        parser.declareField(Builder::setNumAllocations, p -> {
            int value = p.intValue();
            validatePositiveInteger(value, NUM_ALLOCATIONS);
            return value;
        }, new ParseField(NUM_ALLOCATIONS), ObjectParser.ValueType.INT);
        parser.declareField(Builder::setNumThreads, p -> {
            int value = p.intValue();
            validatePositiveInteger(value, NUM_THREADS);
            return value;
        }, new ParseField(NUM_THREADS), ObjectParser.ValueType.INT);
        parser.declareField(Builder::setModelId, p -> {
            String value = p.text();
            validateStringIsNotNullOrEmpty(value, MODEL_ID);
            return value;
        }, new ParseField(MODEL_ID), ObjectParser.ValueType.STRING);
        parser.declareField(Builder::setDeploymentId, p -> {
            String value = p.text();
            validateStringIsNotNullOrEmpty(value, DEPLOYMENT_ID);
            return value;
        }, new ParseField(DEPLOYMENT_ID), ObjectParser.ValueType.STRING);
        parser.declareObject(
            Builder::setAdaptiveAllocationsSettings,
            (p, c) -> parseAdaptiveAllocationsSettings(p),
            new ParseField(ADAPTIVE_ALLOCATIONS)
        );
    }

    private static AdaptiveAllocationsSettings parseAdaptiveAllocationsSettings(XContentParser parser) throws IOException {
        var settings = AdaptiveAllocationsSettings.PARSER.apply(parser, null).build();
        var validationException = settings.validate();
        if (validationException != null) {
            throw validationException;
        }
        return settings;
    }

    public static ElasticsearchInternalServiceSettings fromPersistedMap(Map<String, Object> map) {
        var builder = parseFromMap(map, PERSISTENT_PARSER, ConfigurationParseContext.PERSISTENT);
        validateRequiredFields(builder);
        return builder.build();
    }

    /**
     * Parse the ElasticsearchInternalServiceSettings from the map.
     * Validates that present threading settings are of the right type and value,
     * The model id is optional, it is for the inference service to check and
     * potentially set a default value for the model id.
     * Throws an {@code ValidationException} on validation failures
     *
     * @param map The request map.
     * @return A builder to allow the settings to be modified.
     */
    public static Builder fromRequestMap(Map<String, Object> map) {
        var builder = parseFromMap(map, REQUEST_PARSER, ConfigurationParseContext.REQUEST);
        validateRequiredFields(builder);
        return builder;
    }

    private static Builder parseFromMap(
        Map<String, Object> map,
        ObjectParser<Builder, ConfigurationParseContext> parser,
        ConfigurationParseContext context
    ) {
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, map)) {
            var builder = parser.apply(xParser, context);
            // TODO: remove once all elasticsearch internal service settings are parser-based and ElasticsearchInternalService no
            // longer checks for unconsumed map entries. The object parser reads the map through an XContent view without consuming
            // its entries, so the parsed fields must be removed explicitly to satisfy the caller's check that no unknown settings
            // remain in the map.
            map.remove(NUM_ALLOCATIONS);
            map.remove(NUM_THREADS);
            map.remove(MODEL_ID);
            map.remove(DEPLOYMENT_ID);
            map.remove(ADAPTIVE_ALLOCATIONS);
            return builder;
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse [{}]", e, ModelConfigurations.SERVICE_SETTINGS);
        }
    }

    /**
     * Validates the presence rules that span multiple fields: {@code num_threads} is required and exactly one allocations style
     * (fixed or adaptive) must be given. These checks run after parsing so that services can rely on them regardless of which
     * parser variant populated the builder.
     */
    static void validateRequiredFields(Builder builder) {
        var validationException = new ValidationException();

        if (builder.getNumThreadsOrNull() == null) {
            validationException.addValidationError(missingSettingErrorMsg(NUM_THREADS, ModelConfigurations.SERVICE_SETTINGS));
        }

        if (builder.getNumAllocations() == null && builder.getAdaptiveAllocationsSettings() == null) {
            validationException.addValidationError(
                ServiceUtils.missingOneOfSettingsErrorMsg(
                    List.of(NUM_ALLOCATIONS, ADAPTIVE_ALLOCATIONS),
                    ModelConfigurations.SERVICE_SETTINGS
                )
            );
        }

        validationException.throwIfValidationErrorsExist();
    }

    // TODO: remove once the subclasses with additional fields (reranker, E5, text embedding) declare their fields on a parser
    // created via createParser instead of extracting them from the map.
    protected static Builder fromMap(Map<String, Object> map, ValidationException validationException) {
        Integer numAllocations = extractOptionalPositiveInteger(
            map,
            NUM_ALLOCATIONS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        Integer numThreads = extractRequiredPositiveInteger(map, NUM_THREADS, ModelConfigurations.SERVICE_SETTINGS, validationException);
        AdaptiveAllocationsSettings adaptiveAllocationsSettings = ServiceUtils.removeAsAdaptiveAllocationsSettings(
            map,
            ADAPTIVE_ALLOCATIONS,
            validationException
        );

        // model id is optional as the ELSER service will default it. TODO make this a required field once the elser service is removed
        String modelId = extractOptionalString(map, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);

        if (numAllocations == null && adaptiveAllocationsSettings == null) {
            validationException.addValidationError(
                ServiceUtils.missingOneOfSettingsErrorMsg(
                    List.of(NUM_ALLOCATIONS, ADAPTIVE_ALLOCATIONS),
                    ModelConfigurations.SERVICE_SETTINGS
                )
            );
        }

        String deploymentId = extractOptionalString(map, DEPLOYMENT_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);

        // if an error occurred while parsing, we'll set these to an invalid value, so we don't accidentally get a
        // null pointer when doing unboxing
        return new Builder().setNumAllocations(numAllocations)
            .setNumThreads(Objects.requireNonNullElse(numThreads, FAILED_INT_PARSE_VALUE))
            .setModelId(modelId)
            .setAdaptiveAllocationsSettings(adaptiveAllocationsSettings)
            .setDeploymentId(deploymentId);
    }

    public ElasticsearchInternalServiceSettings(
        @Nullable Integer numAllocations,
        int numThreads,
        String modelId,
        @Nullable AdaptiveAllocationsSettings adaptiveAllocationsSettings,
        @Nullable String deploymentId
    ) {
        this.numAllocations = numAllocations;
        this.numThreads = numThreads;
        this.modelId = Objects.requireNonNull(modelId);
        this.adaptiveAllocationsSettings = adaptiveAllocationsSettings;
        this.deploymentId = deploymentId;
    }

    protected ElasticsearchInternalServiceSettings(ElasticsearchInternalServiceSettings other) {
        this.numAllocations = other.numAllocations;
        this.numThreads = other.numThreads;
        this.modelId = other.modelId;
        this.adaptiveAllocationsSettings = other.adaptiveAllocationsSettings;
        this.deploymentId = other.deploymentId;
    }

    /**
     * Copy constructor with the ability to set the number of allocations. Used for Update API.
     *
     * @param other          the existing settings
     * @param numAllocations the new number of allocations
     */
    public ElasticsearchInternalServiceSettings(ElasticsearchInternalServiceSettings other, int numAllocations) {
        this.numAllocations = numAllocations;
        this.numThreads = other.numThreads;
        this.modelId = other.modelId;
        this.adaptiveAllocationsSettings = other.adaptiveAllocationsSettings;
        this.deploymentId = other.deploymentId;
    }

    public ElasticsearchInternalServiceSettings(StreamInput in) throws IOException {
        this.numAllocations = in.readOptionalVInt();
        this.numThreads = in.readVInt();
        this.modelId = in.readString();
        this.adaptiveAllocationsSettings = in.readOptionalWriteable(AdaptiveAllocationsSettings::new);
        this.deploymentId = in.readOptionalString();
    }

    public void setAllocations(Integer numAllocations, @Nullable AdaptiveAllocationsSettings adaptiveAllocationsSettings) {
        this.numAllocations = numAllocations;
        this.adaptiveAllocationsSettings = adaptiveAllocationsSettings;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalVInt(getNumAllocations());
        out.writeVInt(getNumThreads());
        out.writeString(modelId());
        out.writeOptionalWriteable(getAdaptiveAllocationsSettings());
        out.writeOptionalString(deploymentId);
    }

    @Override
    public String modelId() {
        return modelId;
    }

    public String deloymentId() {
        return modelId;
    }

    public Integer getNumAllocations() {
        return numAllocations;
    }

    public int getNumThreads() {
        return numThreads;
    }

    public AdaptiveAllocationsSettings getAdaptiveAllocationsSettings() {
        return adaptiveAllocationsSettings;
    }

    public String getDeploymentId() {
        return deploymentId;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        addInternalSettingsToXContent(builder, params);
        builder.endObject();
        return builder;
    }

    protected void addInternalSettingsToXContent(XContentBuilder builder, Params params) throws IOException {
        if (numAllocations != null) {
            builder.field(NUM_ALLOCATIONS, numAllocations);
        }
        builder.field(NUM_THREADS, getNumThreads());
        builder.field(MODEL_ID, modelId());
        if (adaptiveAllocationsSettings != null) {
            builder.field(ADAPTIVE_ALLOCATIONS, adaptiveAllocationsSettings);
        }
        if (deploymentId != null) {
            builder.field(DEPLOYMENT_ID, deploymentId);
        }
    }

    @Override
    public ToXContentObject getFilteredXContentObject() {
        return this;
    }

    @Override
    public String getWriteableName() {
        return ElasticsearchInternalServiceSettings.NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }

    /**
     * Parses an update request. Only the allocations settings are mutable: {@code num_threads} is declared solely to reject it
     * with a descriptive error, and any other field is rejected by the strict parser.
     */
    private static class Update {

        private static final ObjectParser<Update, Void> PARSER = new ObjectParser<>(ModelConfigurations.SERVICE_SETTINGS, Update::new);

        private StatefulValue<Integer> numAllocations = StatefulValue.undefined();
        private StatefulValue<AdaptiveAllocationsSettings> adaptiveAllocationsSettings = StatefulValue.undefined();

        static {
            StatefulValue.declareNullable(PARSER, (update, value) -> update.numAllocations = value, p -> {
                int value = p.intValue();
                validatePositiveInteger(value, NUM_ALLOCATIONS);
                return value;
            }, new ParseField(NUM_ALLOCATIONS), ObjectParser.ValueType.INT_OR_NULL);
            StatefulValue.declareNullable(
                PARSER,
                (update, value) -> update.adaptiveAllocationsSettings = value,
                ElasticsearchInternalServiceSettings::parseAdaptiveAllocationsSettings,
                new ParseField(ADAPTIVE_ALLOCATIONS),
                ObjectParser.ValueType.OBJECT_OR_NULL
            );
            PARSER.declareField(
                (update, value) -> {},
                p -> { throw new ElasticsearchParseException("[{}] cannot be updated", NUM_THREADS); },
                new ParseField(NUM_THREADS),
                ObjectParser.ValueType.VALUE
            );
        }

        Builder mergeInto(ElasticsearchInternalServiceSettings existing) {
            var validationException = new ValidationException();

            if (numAllocations.isPresent() == false && adaptiveAllocationsSettings.isPresent() == false) {
                validationException.addValidationError(
                    ServiceUtils.missingOneOfSettingsErrorMsg(
                        List.of(NUM_ALLOCATIONS, ADAPTIVE_ALLOCATIONS),
                        ModelConfigurations.SERVICE_SETTINGS
                    )
                );
            }
            if (numAllocations.isPresent() && adaptiveAllocationsSettings.isPresent()) {
                validationException.addValidationError(
                    Strings.format("[%s] cannot be set if [%s] is set", NUM_ALLOCATIONS, ADAPTIVE_ALLOCATIONS)
                );
            }
            validationException.throwIfValidationErrorsExist();

            return existing.toBuilder()
                .setNumAllocations(numAllocations.orElse(null))
                .setAdaptiveAllocationsSettings(adaptiveAllocationsSettings.orElse(null));
        }
    }

    @Override
    public ServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, serviceSettings)) {
            var update = Update.PARSER.apply(xParser, null);
            // TODO: remove once all elasticsearch internal service settings are parser-based and usesParserForServiceSettings can
            // be enabled on ElasticsearchInternalService. The object parser reads the map through an XContent view without
            // consuming its entries, so the parsed fields must be removed explicitly to satisfy the caller's check that no unknown
            // settings remain in the map.
            serviceSettings.remove(NUM_ALLOCATIONS);
            serviceSettings.remove(ADAPTIVE_ALLOCATIONS);
            return update.mergeInto(this).build();
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse Elasticsearch internal service settings update", e);
        }
    }

    public Builder toBuilder() {
        return new Builder().setAdaptiveAllocationsSettings(adaptiveAllocationsSettings)
            .setDeploymentId(deploymentId)
            .setModelId(modelId)
            .setNumThreads(numThreads)
            .setNumAllocations(numAllocations);
    }

    public static class Builder {
        private Integer numAllocations;
        private Integer numThreads;
        private String modelId;
        private AdaptiveAllocationsSettings adaptiveAllocationsSettings;
        private String deploymentId;

        public ElasticsearchInternalServiceSettings build() {
            // the failed-parse sentinel keeps the legacy map-extraction path building without a null pointer when the accumulated
            // validation errors are thrown after this call; the parser-based paths validate before building
            return new ElasticsearchInternalServiceSettings(
                numAllocations,
                Objects.requireNonNullElse(numThreads, FAILED_INT_PARSE_VALUE),
                modelId,
                adaptiveAllocationsSettings,
                deploymentId
            );
        }

        public Builder setNumAllocations(Integer numAllocations) {
            this.numAllocations = numAllocations;
            return this;
        }

        public Builder setNumThreads(int numThreads) {
            this.numThreads = numThreads;
            return this;
        }

        public Builder setModelId(String modelId) {
            this.modelId = modelId;
            return this;
        }

        public Builder setDeploymentId(String deploymentId) {
            this.deploymentId = deploymentId;
            return this;
        }

        public Builder setAdaptiveAllocationsSettings(AdaptiveAllocationsSettings adaptiveAllocationsSettings) {
            this.adaptiveAllocationsSettings = adaptiveAllocationsSettings;
            return this;
        }

        public String getModelId() {
            return modelId;
        }

        public Integer getNumAllocations() {
            return numAllocations;
        }

        public int getNumThreads() {
            return numThreads;
        }

        Integer getNumThreadsOrNull() {
            return numThreads;
        }

        public AdaptiveAllocationsSettings getAdaptiveAllocationsSettings() {
            return adaptiveAllocationsSettings;
        }
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ElasticsearchInternalServiceSettings that = (ElasticsearchInternalServiceSettings) o;
        return Objects.equals(numAllocations, that.numAllocations)
            && numThreads == that.numThreads
            && Objects.equals(modelId, that.modelId)
            && Objects.equals(adaptiveAllocationsSettings, that.adaptiveAllocationsSettings)
            && Objects.equals(deploymentId, that.deploymentId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(numAllocations, numThreads, modelId, adaptiveAllocationsSettings, deploymentId);
    }
}
