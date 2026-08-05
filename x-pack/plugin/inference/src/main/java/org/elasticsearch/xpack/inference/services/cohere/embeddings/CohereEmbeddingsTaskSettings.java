/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.embeddings;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.inference.common.model.Truncation;
import org.elasticsearch.xpack.inference.common.parser.EnumParser;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.inference.InputType.invalidInputTypeMessage;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.inference.services.cohere.CohereService.VALID_INPUT_TYPE_VALUES;
import static org.elasticsearch.xpack.inference.services.cohere.CohereServiceFields.TRUNCATE;

/**
 * Defines the task settings for the cohere text embeddings service.
 *
 * <p>
 * <a href="https://docs.cohere.com/reference/embed">See api docs for details.</a>
 * </p>
 */
public class CohereEmbeddingsTaskSettings implements TaskSettings {

    public static final String NAME = "cohere_embeddings_task_settings";
    public static final CohereEmbeddingsTaskSettings EMPTY_SETTINGS = new CohereEmbeddingsTaskSettings(null, null);
    static final String INPUT_TYPE = "input_type";

    private static final ConstructingObjectParser<CohereEmbeddingsTaskSettings, Void> REQUEST_PARSER = createParser(false);
    private static final ConstructingObjectParser<CohereEmbeddingsTaskSettings, Void> PERSISTENT_PARSER = createParser(true);

    static ConstructingObjectParser<CohereEmbeddingsTaskSettings, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<CohereEmbeddingsTaskSettings, Void> parser = new ConstructingObjectParser<>(
            ModelConfigurations.TASK_SETTINGS,
            ignoreUnknownFields,
            args -> {
                InputType inputType = EnumParser.parseFromStringInObjectParserContext(
                    (String) args[0],
                    InputType::fromString,
                    VALID_INPUT_TYPE_VALUES,
                    EnumSet.of(InputType.INTERNAL_INGEST, InputType.INTERNAL_SEARCH)
                );
                Truncation truncation = args[1] == null ? null : Truncation.fromString((String) args[1]);
                return new CohereEmbeddingsTaskSettings(inputType, truncation);
            }
        );
        parser.declareString(optionalConstructorArg(), new ParseField(INPUT_TYPE));
        parser.declareString(optionalConstructorArg(), new ParseField(TRUNCATE));
        return parser;
    }

    public static CohereEmbeddingsTaskSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        if (map == null || map.isEmpty()) {
            return EMPTY_SETTINGS;
        }
        var parser = context == ConfigurationParseContext.REQUEST ? REQUEST_PARSER : PERSISTENT_PARSER;
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, map)) {
            return parser.apply(xParser, null);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse [{}]", e, ModelConfigurations.TASK_SETTINGS);
        }
    }

    /**
     * Creates a new {@link CohereEmbeddingsTaskSettings} by preferring non-null fields from the provided parameters.
     * For the input type, preference is given to requestInputType if it is not null and not UNSPECIFIED.
     * Then preference is given to the requestTaskSettings and finally to originalSettings even if the value is null.
     * <p>
     * Similarly, for the truncation field preference is given to requestTaskSettings if it is not null and then to
     * originalSettings.
     *
     * @param originalSettings    the settings stored as part of the inference entity configuration
     * @param requestTaskSettings the settings passed in within the task_settings field of the request
     * @return a constructed {@link CohereEmbeddingsTaskSettings}
     */
    public static CohereEmbeddingsTaskSettings of(
        CohereEmbeddingsTaskSettings originalSettings,
        CohereEmbeddingsTaskSettings requestTaskSettings
    ) {
        var inputTypeToUse = getValidInputType(originalSettings, requestTaskSettings);
        var truncationToUse = getValidTruncation(originalSettings, requestTaskSettings);

        return new CohereEmbeddingsTaskSettings(inputTypeToUse, truncationToUse);
    }

    private static InputType getValidInputType(
        CohereEmbeddingsTaskSettings originalSettings,
        CohereEmbeddingsTaskSettings requestTaskSettings
    ) {
        InputType inputTypeToUse = originalSettings.inputType;

        // prefer input type in request task settings over input type in persisted task settings
        if (requestTaskSettings.inputType != null) {
            inputTypeToUse = requestTaskSettings.inputType;
        }

        return inputTypeToUse;
    }

    private static Truncation getValidTruncation(
        CohereEmbeddingsTaskSettings originalSettings,
        CohereEmbeddingsTaskSettings requestTaskSettings
    ) {
        return requestTaskSettings.getTruncation() == null ? originalSettings.truncation : requestTaskSettings.getTruncation();
    }

    private final InputType inputType;
    private final Truncation truncation;

    public CohereEmbeddingsTaskSettings(StreamInput in) throws IOException {
        this(in.readOptionalEnum(InputType.class), in.readOptionalEnum(Truncation.class));
    }

    public CohereEmbeddingsTaskSettings(@Nullable InputType inputType, @Nullable Truncation truncation) {
        validateInputType(inputType);
        this.inputType = inputType;
        this.truncation = truncation;
    }

    private static void validateInputType(InputType inputType) {
        if (inputType == null) {
            return;
        }

        assert VALID_INPUT_TYPE_VALUES.contains(inputType) : invalidInputTypeMessage(inputType);
    }

    @Override
    public boolean isEmpty() {
        return inputType == null && truncation == null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (inputType != null) {
            builder.field(INPUT_TYPE, inputType);
        }

        if (truncation != null) {
            builder.field(TRUNCATE, truncation);
        }
        builder.endObject();
        return builder;
    }

    public InputType getInputType() {
        return inputType;
    }

    public Truncation getTruncation() {
        return truncation;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalEnum(inputType);
        out.writeOptionalEnum(truncation);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CohereEmbeddingsTaskSettings that = (CohereEmbeddingsTaskSettings) o;
        return Objects.equals(inputType, that.inputType) && Objects.equals(truncation, that.truncation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(inputType, truncation);
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        CohereEmbeddingsTaskSettings updatedSettings = CohereEmbeddingsTaskSettings.fromMap(newSettings, ConfigurationParseContext.REQUEST);
        return of(this, updatedSettings);
    }
}
