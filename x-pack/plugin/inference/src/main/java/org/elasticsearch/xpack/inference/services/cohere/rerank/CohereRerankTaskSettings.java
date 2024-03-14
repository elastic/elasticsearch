/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.rerank;

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
import org.elasticsearch.xpack.inference.services.cohere.CohereTruncation;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalBoolean;

/**
 * Defines the task settings for the cohere rerank service.
 *
 * <p>
 * <a href="https://docs.cohere.com/reference/rerank-1">See api docs for details.</a>
 * </p>
 */
public class CohereRerankTaskSettings implements TaskSettings {

    public static final String NAME = "cohere_rerank_task_settings";
    public static final CohereRerankTaskSettings EMPTY_SETTINGS = new CohereRerankTaskSettings(null, null);
    static final String RETURN_DOCUMENTS = "return_documents";

    public static CohereRerankTaskSettings fromMap(Map<String, Object> map) {
        if (map == null || map.isEmpty()) {
            return EMPTY_SETTINGS;
        }

        ValidationException validationException = new ValidationException();

        Boolean returnDocuments = extractOptionalBoolean(map, RETURN_DOCUMENTS, ModelConfigurations.TASK_SETTINGS, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return of(returnDocuments);
    }

    /**
     * Creates a new {@link CohereRerankTaskSettings} by preferring non-null fields from the provided parameters.
     * For returnDocuments, preference is given to requestTaskSettings if it is not null. Otherwise, preference is given to true.
     *
     * Similarly, for the truncation field preference is given to requestTaskSettings if it is not null and then to
     * originalSettings.
     * @param originalSettings the settings stored as part of the inference entity configuration
     * @param requestTaskSettings the settings passed in within the task_settings field of the request
     * @param returnDocuments if the response should include the text of each document, else the response will only include index, and
     *                        relevance score
     * @return a constructed {@link CohereRerankTaskSettings}
     */
    public static CohereRerankTaskSettings of(
        CohereRerankTaskSettings originalSettings,
        CohereRerankTaskSettings requestTaskSettings,
        Boolean returnDocuments
    ) {
        Boolean doesReturnDocuments;
        if (requestTaskSettings != null && requestTaskSettings.getDoesReturnDocuments() != null) {
            doesReturnDocuments = requestTaskSettings.getDoesReturnDocuments();
        } else {
            doesReturnDocuments = returnDocuments || (originalSettings != null && originalSettings.getDoesReturnDocuments());
        }
        var truncationToUse = getValidTruncation(originalSettings, requestTaskSettings);

        return new CohereRerankTaskSettings(doesReturnDocuments, truncationToUse);
    }

    public static CohereRerankTaskSettings of(Boolean returnDocuments) {
        return new CohereRerankTaskSettings(returnDocuments, null);
    }

    private static CohereTruncation getValidTruncation(
        CohereRerankTaskSettings originalSettings,
        CohereRerankTaskSettings requestTaskSettings
    ) {
        return requestTaskSettings.getTruncation() == null ? originalSettings.truncation : requestTaskSettings.getTruncation();
    }

    private final Boolean returnDocuments;
    private final CohereTruncation truncation;

    public CohereRerankTaskSettings(StreamInput in) throws IOException {
        this(in.readBoolean(), in.readOptionalEnum(CohereTruncation.class));
    }

    public CohereRerankTaskSettings(@Nullable Boolean doReturnDocuments, @Nullable CohereTruncation truncation) {
        this.returnDocuments = doReturnDocuments;
        this.truncation = truncation;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (returnDocuments != null) {
            builder.field(RETURN_DOCUMENTS, returnDocuments);
        }
        builder.endObject();
        return builder;
    }

    public Boolean getDoesReturnDocuments() {
        return returnDocuments;
    }

    public CohereTruncation getTruncation() {
        return truncation;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ML_INFERENCE_COHERE_RERANK;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalEnum(truncation);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CohereRerankTaskSettings that = (CohereRerankTaskSettings) o;
        return Objects.equals(returnDocuments, that.returnDocuments) && Objects.equals(truncation, that.truncation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(returnDocuments, truncation);
    }

    public static String invalidInputTypeMessage(InputType inputType) {
        return Strings.format("received invalid input type value [%s]", inputType.toString());
    }
}
