/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.rerank;

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
import static org.elasticsearch.xpack.inference.services.mixedbread.MixedbreadConstants.RETURN_DOCUMENTS_FIELD;
import static org.elasticsearch.xpack.inference.services.mixedbread.MixedbreadConstants.TOP_K_FIELD;

public class MixedbreadRerankTaskSettings implements TaskSettings {
    public static final String NAME = "mixedbread_rerank_task_settings";

    public static MixedbreadRerankTaskSettings fromMap(Map<String, Object> map) {
        final var validationException = new ValidationException();

        final var returnDocuments = extractOptionalBoolean(map, RETURN_DOCUMENTS_FIELD, validationException);
        final var topN = extractOptionalPositiveInteger(map, TOP_K_FIELD, ModelConfigurations.TASK_SETTINGS, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new MixedbreadRerankTaskSettings(returnDocuments, topN);
    }

    /**
     * Creates a new {@link MixedbreadRerankTaskSettings} object by overriding the values in originalSettings with the ones
     * passed in via requestSettings if the fields are not null.
     * @param originalSettings the original {@link MixedbreadRerankTaskSettings} from the inference entity configuration from storage
     * @param requestSettings the {@link MixedbreadRerankRequestTaskSettings} from the request
     * @return a new {@link MixedbreadRerankTaskSettings}
     */
    public static MixedbreadRerankTaskSettings of(
        MixedbreadRerankTaskSettings originalSettings,
        MixedbreadRerankRequestTaskSettings requestSettings
    ) {

        final var returnDocuments = requestSettings.returnDocuments() == null
            ? originalSettings.returnDocuments()
            : requestSettings.returnDocuments();
        final var topK = requestSettings.topN() == null ? originalSettings.topK() : requestSettings.topN();

        return new MixedbreadRerankTaskSettings(returnDocuments, topK);
    }

    public MixedbreadRerankTaskSettings(@Nullable Boolean returnDocuments, @Nullable Integer topK) {
        this.returnDocuments = returnDocuments;
        this.topK = topK;
    }

    public MixedbreadRerankTaskSettings(StreamInput in) throws IOException {
        this.returnDocuments = in.readOptionalBoolean();
        this.topK = in.readOptionalVInt();
    }

    private final Boolean returnDocuments;
    private final Integer topK;

    public Boolean returnDocuments() {
        return returnDocuments;
    }

    public Integer topK() {
        return topK;
    }

    public boolean areAnyParametersAvailable() {
        return returnDocuments != null && topK != null;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ML_INFERENCE_MIXEDBREAD_ADDED;
    }

    @Override
    public boolean isEmpty() {
        return returnDocuments == null && topK == null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalBoolean(returnDocuments);
        out.writeOptionalVInt(topK);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (returnDocuments != null) {
            builder.field(RETURN_DOCUMENTS_FIELD, returnDocuments);
        }
        if (topK != null) {
            builder.field(TOP_K_FIELD, topK);
        }

        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return "MixedbreadRerankTaskSettings{" + ", returnDocuments=" + returnDocuments + ", topN=" + topK + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MixedbreadRerankTaskSettings that = (MixedbreadRerankTaskSettings) o;
        return Objects.equals(returnDocuments, that.returnDocuments) && Objects.equals(topK, that.topK);
    }

    @Override
    public int hashCode() {
        return Objects.hash(returnDocuments, topK);
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        MixedbreadRerankRequestTaskSettings requestSettings = MixedbreadRerankRequestTaskSettings.fromMap(new HashMap<>(newSettings));
        return of(this, requestSettings);
    }
}
