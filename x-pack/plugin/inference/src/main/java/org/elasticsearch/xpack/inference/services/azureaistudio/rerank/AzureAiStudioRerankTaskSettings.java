/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalBoolean;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.RETURN_DOCUMENTS_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.TOP_N_FIELD;

/**
 * Defines the task settings for the AzureAiStudio cohere rerank service.
 *
 * <p>
 * <a href="https://docs.cohere.com/v1/reference/rerank">See api docs for details.</a>
 * </p>
 */
public class AzureAiStudioRerankTaskSettings implements TaskSettings {
    public static final String NAME = "azure_ai_studio_rerank_task_settings";

    public static AzureAiStudioRerankTaskSettings fromMap(Map<String, Object> map) {
        final var validationException = new ValidationException();

        final var returnDocuments = extractOptionalBoolean(map, RETURN_DOCUMENTS_FIELD, validationException);
        final var topN = extractOptionalPositiveInteger(map, TOP_N_FIELD, ModelConfigurations.TASK_SETTINGS, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new AzureAiStudioRerankTaskSettings(returnDocuments, topN);
    }

    /**
     * Creates a new {@link AzureAiStudioRerankTaskSettings} object by overriding the values in originalSettings with the ones
     * passed in via requestSettings if the fields are not null.
     * @param originalSettings the original {@link AzureAiStudioRerankTaskSettings} from the inference entity configuration from storage
     * @param requestSettings the {@link AzureAiStudioRerankTaskSettings} from the request
     * @return a new {@link AzureAiStudioRerankTaskSettings}
     */
    public static AzureAiStudioRerankTaskSettings of(
        AzureAiStudioRerankTaskSettings originalSettings,
        AzureAiStudioRerankRequestTaskSettings requestSettings
    ) {

        final var returnDocuments = requestSettings.returnDocuments() == null
            ? originalSettings.returnDocuments()
            : requestSettings.returnDocuments();
        final var topN = requestSettings.topN() == null ? originalSettings.topN() : requestSettings.topN();

        return new AzureAiStudioRerankTaskSettings(returnDocuments, topN);
    }

    public AzureAiStudioRerankTaskSettings(@Nullable Boolean returnDocuments, @Nullable Integer topN) {
        this.returnDocuments = returnDocuments;
        this.topN = topN;
    }

    public AzureAiStudioRerankTaskSettings(StreamInput in) throws IOException {
        this.returnDocuments = in.readOptionalBoolean();
        this.topN = in.readOptionalVInt();
    }

    private final Boolean returnDocuments;
    private final Integer topN;

    public Boolean returnDocuments() {
        return returnDocuments;
    }

    public Integer topN() {
        return topN;
    }

    public boolean areAnyParametersAvailable() {
        return returnDocuments != null && topN != null;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ML_INFERENCE_AZURE_AI_STUDIO_ADDED;
    }

    @Override
    public boolean isEmpty() {
        return returnDocuments == null && topN == null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalBoolean(returnDocuments);
        out.writeOptionalVInt(topN);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (returnDocuments != null) {
            builder.field(RETURN_DOCUMENTS_FIELD, returnDocuments);
        }
        if (topN != null) {
            builder.field(TOP_N_FIELD, topN);
        }

        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return "AzureAiStudioRerankTaskSettings{" + ", returnDocuments=" + returnDocuments + ", topN=" + topN + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AzureAiStudioRerankTaskSettings that = (AzureAiStudioRerankTaskSettings) o;
        return Objects.equals(returnDocuments, that.returnDocuments) && Objects.equals(topN, that.topN);
    }

    @Override
    public int hashCode() {
        return Objects.hash(returnDocuments, topN);
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        AzureAiStudioRerankRequestTaskSettings requestSettings = AzureAiStudioRerankRequestTaskSettings.fromMap(new HashMap<>(newSettings));
        return of(this, requestSettings);
    }
}
