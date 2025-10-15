/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.openshiftai.OpenShiftAiUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalBoolean;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;

/**
 * Defines the task settings for the OpenShift AI rerank service.
 */
public class OpenShiftAIRerankTaskSettings implements TaskSettings {

    public static final String NAME = "openshift_ai_rerank_task_settings";
    public static final String RETURN_DOCUMENTS = "return_documents";
    public static final String TOP_N = "top_n";

    private static final OpenShiftAIRerankTaskSettings EMPTY_SETTINGS = new OpenShiftAIRerankTaskSettings(null, null);

    /**
     * Creates a new {@link OpenShiftAIRerankTaskSettings} from a map of settings.
     * @param map the map of settings
     * @return a constructed {@link OpenShiftAIRerankTaskSettings}
     * @throws ValidationException if any of the settings are invalid
     */
    public static OpenShiftAIRerankTaskSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        if (map == null || map.isEmpty()) {
            return EMPTY_SETTINGS;
        }

        Boolean returnDocuments = extractOptionalBoolean(map, RETURN_DOCUMENTS, validationException);
        Integer topN = extractOptionalPositiveInteger(map, TOP_N, ModelConfigurations.TASK_SETTINGS, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return of(topN, returnDocuments);
    }

    /**
     * Creates a new {@link OpenShiftAIRerankTaskSettings} by using non-null fields from the request settings over the original settings.
     *
     * @param originalSettings    the settings stored as part of the inference entity configuration
     * @param requestTaskSettings the settings passed in within the task_settings field of the request
     * @return a constructed {@link OpenShiftAIRerankTaskSettings}
     */
    public static OpenShiftAIRerankTaskSettings of(
        OpenShiftAIRerankTaskSettings originalSettings,
        OpenShiftAIRerankTaskSettings requestTaskSettings
    ) {
        return new OpenShiftAIRerankTaskSettings(
            requestTaskSettings.getTopN() != null ? requestTaskSettings.getTopN() : originalSettings.getTopN(),
            requestTaskSettings.getReturnDocuments() != null
                ? requestTaskSettings.getReturnDocuments()
                : originalSettings.getReturnDocuments()
        );
    }

    /**
     * Creates a new {@link OpenShiftAIRerankTaskSettings} with the specified settings.
     *
     * @param topN            the number of top documents to return
     * @param returnDocuments whether to return the documents
     * @return a constructed {@link OpenShiftAIRerankTaskSettings}
     */
    public static OpenShiftAIRerankTaskSettings of(@Nullable Integer topN, @Nullable Boolean returnDocuments) {
        return new OpenShiftAIRerankTaskSettings(topN, returnDocuments);
    }

    private final Integer topN;
    private final Boolean returnDocuments;

    /**
     * Constructs a new {@link OpenShiftAIRerankTaskSettings} by reading from a {@link StreamInput}.
     *
     * @param in the stream input to read from
     * @throws IOException if an I/O error occurs
     */
    public OpenShiftAIRerankTaskSettings(StreamInput in) throws IOException {
        this(in.readOptionalInt(), in.readOptionalBoolean());
    }

    /**
     * Constructs a new {@link OpenShiftAIRerankTaskSettings} with the specified settings.
     *
     * @param topN            the number of top documents to return
     * @param doReturnDocuments whether to return the documents
     */
    public OpenShiftAIRerankTaskSettings(@Nullable Integer topN, @Nullable Boolean doReturnDocuments) {
        this.topN = topN;
        this.returnDocuments = doReturnDocuments;
    }

    @Override
    public boolean isEmpty() {
        return topN == null && returnDocuments == null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (topN != null) {
            builder.field(TOP_N, topN);
        }
        if (returnDocuments != null) {
            builder.field(RETURN_DOCUMENTS, returnDocuments);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        assert false : "should never be called when supportsVersion is used";
        return OpenShiftAiUtils.ML_INFERENCE_OPENSHIFT_AI_ADDED;
    }

    @Override
    public boolean supportsVersion(TransportVersion version) {
        return OpenShiftAiUtils.supportsOpenShiftAi(version);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalInt(topN);
        out.writeOptionalBoolean(returnDocuments);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OpenShiftAIRerankTaskSettings that = (OpenShiftAIRerankTaskSettings) o;
        return Objects.equals(returnDocuments, that.returnDocuments) && Objects.equals(topN, that.topN);
    }

    @Override
    public int hashCode() {
        return Objects.hash(returnDocuments, topN);
    }

    public Integer getTopN() {
        return topN;
    }

    public Boolean getReturnDocuments() {
        return returnDocuments;
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        OpenShiftAIRerankTaskSettings updatedSettings = OpenShiftAIRerankTaskSettings.fromMap(new HashMap<>(newSettings));
        return OpenShiftAIRerankTaskSettings.of(this, updatedSettings);
    }
}
