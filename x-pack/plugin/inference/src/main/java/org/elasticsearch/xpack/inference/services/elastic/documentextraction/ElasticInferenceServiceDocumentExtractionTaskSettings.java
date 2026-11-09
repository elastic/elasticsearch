/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.documentextraction;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceUtils;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.SettingsScope.TASK_SETTINGS;

/**
 * Task settings for the Elastic Inference Service {@code document_extraction} task type. They can be stored on the inference endpoint
 * and overridden per request through the {@code task_settings} field of the document extraction request body, where the request value
 * wins (see {@link #of(ElasticInferenceServiceDocumentExtractionTaskSettings, ElasticInferenceServiceDocumentExtractionTaskSettings)}).
 * The resolved settings are forwarded to the Elastic Inference Service as-is:
 * <pre>
 * {
 *   "output_format": "markdown",
 *   "parameters": {
 *     "preset": "ocr",
 *     "page": 2
 *   }
 * }</pre>
 * {@code parameters} is a free-form object of extraction options. Elasticsearch only validates that it is an object: the Elastic
 * Inference Service owns the set of accepted parameters, so it can add, tune or remove them without a change here.
 */
public class ElasticInferenceServiceDocumentExtractionTaskSettings implements TaskSettings {

    public static final String NAME = "elastic_inference_service_document_extraction_task_settings";
    public static final String OUTPUT_FORMAT = "output_format";
    public static final String PARAMETERS = "parameters";

    private static final TransportVersion INFERENCE_API_EIS_DOCUMENT_EXTRACTION_ADDED = TransportVersion.fromName(
        "inference_api_eis_document_extraction_added"
    );
    /**
     * Adds {@code parameters} and drops the {@code css} settings that earlier versions serialized.
     */
    private static final TransportVersion INFERENCE_API_EIS_DOCUMENT_EXTRACTION_PARAMETERS = TransportVersion.fromName(
        "inference_api_eis_document_extraction_parameters"
    );

    public static final ElasticInferenceServiceDocumentExtractionTaskSettings EMPTY_SETTINGS =
        new ElasticInferenceServiceDocumentExtractionTaskSettings(null, Map.of());

    private static final ObjectParser<Builder, Void> REQUEST_PARSER = createParser(false);
    private static final ObjectParser<Builder, Void> PERSISTENT_PARSER = createParser(true);

    private static ObjectParser<Builder, Void> createParser(boolean ignoreUnknownFields) {
        var parser = new ObjectParser<Builder, Void>(TASK_SETTINGS.toString(), ignoreUnknownFields, Builder::new);
        parser.declareString(Builder::setOutputFormat, new ParseField(OUTPUT_FORMAT));
        // Parsed as a plain map rather than through a parser so that its content is never validated here, also not in the request
        // context: the accepted parameters are defined by the Elastic Inference Service
        parser.declareField(Builder::setParameters, (p, c) -> p.mapOrdered(), new ParseField(PARAMETERS), ObjectParser.ValueType.OBJECT);
        return parser;
    }

    /**
     * Parses task settings from a raw config map. Unknown fields are rejected for {@link ConfigurationParseContext#REQUEST} and ignored
     * for {@link ConfigurationParseContext#PERSISTENT}, so settings persisted by a newer version can still be read. A null or empty map
     * produces {@link #EMPTY_SETTINGS}.
     */
    public static ElasticInferenceServiceDocumentExtractionTaskSettings fromMap(
        @Nullable Map<String, Object> map,
        ConfigurationParseContext context
    ) {
        if (map == null || map.isEmpty()) {
            return EMPTY_SETTINGS;
        }

        var parser = context == ConfigurationParseContext.REQUEST ? REQUEST_PARSER : PERSISTENT_PARSER;
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, map)) {
            return parser.apply(xParser, null).build();
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse [{}]", e, TASK_SETTINGS);
        }
    }

    /**
     * Merges stored and request task settings: a field set in {@code requestSettings} overrides the stored value, otherwise the stored
     * value is kept. {@code parameters} are merged per key, so a request can override one parameter while keeping the other stored ones.
     */
    public static ElasticInferenceServiceDocumentExtractionTaskSettings of(
        ElasticInferenceServiceDocumentExtractionTaskSettings originalSettings,
        ElasticInferenceServiceDocumentExtractionTaskSettings requestSettings
    ) {
        var parameters = new LinkedHashMap<>(originalSettings.parameters);
        parameters.putAll(requestSettings.parameters);
        return new ElasticInferenceServiceDocumentExtractionTaskSettings(
            requestSettings.outputFormat != null ? requestSettings.outputFormat : originalSettings.outputFormat,
            parameters
        );
    }

    private final String outputFormat;
    private final Map<String, Object> parameters;

    public ElasticInferenceServiceDocumentExtractionTaskSettings(StreamInput in) throws IOException {
        this.outputFormat = in.readOptionalString();
        this.parameters = in.readGenericMap();
    }

    public ElasticInferenceServiceDocumentExtractionTaskSettings(@Nullable String outputFormat) {
        this(outputFormat, Map.of());
    }

    /**
     * @param parameters free-form extraction options; an empty map means none are set
     */
    public ElasticInferenceServiceDocumentExtractionTaskSettings(@Nullable String outputFormat, Map<String, Object> parameters) {
        this.outputFormat = outputFormat;
        this.parameters = Objects.requireNonNull(parameters);
    }

    @Nullable
    public String outputFormat() {
        return outputFormat;
    }

    public Map<String, Object> parameters() {
        return parameters;
    }

    @Override
    public boolean isEmpty() {
        return Strings.isNullOrEmpty(outputFormat) && parameters.isEmpty();
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        return of(this, fromMap(newSettings, ConfigurationParseContext.REQUEST));
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return INFERENCE_API_EIS_DOCUMENT_EXTRACTION_PARAMETERS;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(outputFormat);
        out.writeGenericMap(parameters);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        toXContentFragment(builder, params);
        builder.endObject();
        return builder;
    }

    /**
     * Writes the individual settings as fields of the object the {@code builder} is currently in, without wrapping them in an object
     * of their own. The Elastic Inference Service expects the settings as top-level fields of its request body rather than nested
     * under {@code task_settings}, so the request entity uses this to inline them. Unset settings are skipped.
     */
    public XContentBuilder toXContentFragment(XContentBuilder builder, Params params) throws IOException {
        if (Strings.isNullOrEmpty(outputFormat) == false) {
            builder.field(OUTPUT_FORMAT, outputFormat);
        }
        if (parameters.isEmpty() == false) {
            builder.field(PARAMETERS, parameters);
        }
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ElasticInferenceServiceDocumentExtractionTaskSettings that = (ElasticInferenceServiceDocumentExtractionTaskSettings) o;
        return Objects.equals(outputFormat, that.outputFormat) && Objects.equals(parameters, that.parameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(outputFormat, parameters);
    }

    private static class Builder {
        private String outputFormat;
        private Map<String, Object> parameters = Map.of();

        private void setOutputFormat(String outputFormat) {
            this.outputFormat = outputFormat;
        }

        private void setParameters(Map<String, Object> parameters) {
            this.parameters = parameters;
        }

        private ElasticInferenceServiceDocumentExtractionTaskSettings build() {
            if (outputFormat != null && outputFormat.isEmpty()) {
                var validationException = new ValidationException();
                validationException.addValidationError(ServiceUtils.mustBeNonEmptyString(OUTPUT_FORMAT, TASK_SETTINGS));
                throw validationException;
            }
            return new ElasticInferenceServiceDocumentExtractionTaskSettings(outputFormat, parameters);
        }
    }
}
