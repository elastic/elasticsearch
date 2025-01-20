/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.sparse;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalBoolean;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalEnum;

/**
 * Defines the task settings for the alibabacloud search text sparse embeddings service.
 *
 * <p>
 * <a href="https://help.aliyun.com/zh/open-search/search-platform/developer-reference/text-sparse-embedding-api-details">
 * See api docs for details.</a>
 * </p>
 */
public class AlibabaCloudSearchSparseTaskSettings implements TaskSettings {

    public static final String NAME = "alibabacloud_search_sparse_embeddings_task_settings";
    public static final AlibabaCloudSearchSparseTaskSettings EMPTY_SETTINGS = new AlibabaCloudSearchSparseTaskSettings(null, null);
    static final String INPUT_TYPE = "input_type";
    static final String RETURN_TOKEN = "return_token";
    static final EnumSet<InputType> VALID_REQUEST_VALUES = EnumSet.of(InputType.INGEST, InputType.SEARCH);

    public static AlibabaCloudSearchSparseTaskSettings fromMap(Map<String, Object> map) {
        if (map == null || map.isEmpty()) {
            return EMPTY_SETTINGS;
        }

        ValidationException validationException = new ValidationException();

        InputType inputType = extractOptionalEnum(
            map,
            INPUT_TYPE,
            ModelConfigurations.TASK_SETTINGS,
            InputType::fromString,
            VALID_REQUEST_VALUES,
            validationException
        );

        Boolean returnToken = extractOptionalBoolean(map, RETURN_TOKEN, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new AlibabaCloudSearchSparseTaskSettings(inputType, returnToken);
    }

    /**
     * Creates a new {@link AlibabaCloudSearchSparseTaskSettings} by preferring non-null fields from the provided parameters.
     * For the input type, preference is given to requestInputType if it is not null and not UNSPECIFIED.
     * Then preference is given to the requestTaskSettings and finally to originalSettings even if the value is null.
     * <p>
     * Similarly, for the truncation field preference is given to requestTaskSettings if it is not null and then to
     * originalSettings.
     *
     * @param originalSettings    the settings stored as part of the inference entity configuration
     * @param requestTaskSettings the settings passed in within the task_settings field of the request
     * @param requestInputType    the input type passed in the request parameters
     * @return a constructed {@link AlibabaCloudSearchSparseTaskSettings}
     */
    public static AlibabaCloudSearchSparseTaskSettings of(
        AlibabaCloudSearchSparseTaskSettings originalSettings,
        AlibabaCloudSearchSparseTaskSettings requestTaskSettings,
        InputType requestInputType
    ) {
        var inputTypeToUse = getValidInputType(originalSettings, requestTaskSettings, requestInputType);
        var returnToken = requestTaskSettings.isReturnToken() != null
            ? requestTaskSettings.isReturnToken()
            : originalSettings.isReturnToken();
        return new AlibabaCloudSearchSparseTaskSettings(inputTypeToUse, returnToken);
    }

    private static InputType getValidInputType(
        AlibabaCloudSearchSparseTaskSettings originalSettings,
        AlibabaCloudSearchSparseTaskSettings requestTaskSettings,
        InputType requestInputType
    ) {
        InputType inputTypeToUse = originalSettings.inputType;

        if (VALID_REQUEST_VALUES.contains(requestInputType)) {
            inputTypeToUse = requestInputType;
        } else if (requestTaskSettings.inputType != null) {
            inputTypeToUse = requestTaskSettings.inputType;
        }

        return inputTypeToUse;
    }

    private final InputType inputType;
    private final Boolean returnToken;

    public AlibabaCloudSearchSparseTaskSettings(StreamInput in) throws IOException {
        this(in.readOptionalEnum(InputType.class), in.readOptionalBoolean());
    }

    public AlibabaCloudSearchSparseTaskSettings(@Nullable InputType inputType, Boolean returnToken) {
        validateInputType(inputType);
        this.inputType = inputType;
        this.returnToken = returnToken;
    }

    private static void validateInputType(InputType inputType) {
        if (inputType == null) {
            return;
        }

        assert VALID_REQUEST_VALUES.contains(inputType) : invalidInputTypeMessage(inputType);
    }

    @Override
    public boolean isEmpty() {
        return inputType == null && returnToken == null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (inputType != null) {
            builder.field(INPUT_TYPE, inputType);
        }
        if (returnToken != null) {
            builder.field(RETURN_TOKEN, returnToken);
        }
        builder.endObject();
        return builder;
    }

    public InputType getInputType() {
        return inputType;
    }

    public Boolean isReturnToken() {
        return returnToken;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_16_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalEnum(inputType);
        out.writeOptionalBoolean(returnToken);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AlibabaCloudSearchSparseTaskSettings that = (AlibabaCloudSearchSparseTaskSettings) o;
        return Objects.equals(inputType, that.inputType) && Objects.equals(returnToken, that.returnToken);
    }

    @Override
    public int hashCode() {
        return Objects.hash(inputType, returnToken);
    }

    public static String invalidInputTypeMessage(InputType inputType) {
        return Strings.format("received invalid input type value [%s]", inputType.toString());
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        AlibabaCloudSearchSparseTaskSettings updatedSettings = fromMap(new HashMap<>(newSettings));
        return of(this, updatedSettings, updatedSettings.getInputType() != null ? updatedSettings.getInputType() : this.inputType);
    }
}
