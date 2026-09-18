/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TopNProvider;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalBoolean;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.SettingsScope.TASK_SETTINGS;
import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiServiceFields.RETURN_DOCUMENTS;
import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiServiceFields.TOP_N;

/**
 * Task settings of the OCI Generative AI {@code rerank} task.
 *
 * @see <a href="https://docs.oracle.com/en-us/iaas/api/#/en/generative-ai-inference/20231130/datatypes/RerankTextDetails">RerankTextDetails</a>
 */
public class OciGenAiRerankTaskSettings implements TaskSettings, TopNProvider {

    public static final String NAME = "oci_genai_rerank_task_settings";
    public static final OciGenAiRerankTaskSettings EMPTY_SETTINGS = new OciGenAiRerankTaskSettings(null, null);

    public static OciGenAiRerankTaskSettings fromMap(@Nullable Map<String, Object> map) {
        if (map == null || map.isEmpty()) {
            return EMPTY_SETTINGS;
        }

        var validationException = new ValidationException();
        var topN = extractOptionalPositiveInteger(map, TOP_N, TASK_SETTINGS, validationException);
        var returnDocuments = extractOptionalBoolean(map, RETURN_DOCUMENTS, validationException);
        validationException.throwIfValidationErrorsExist();

        return new OciGenAiRerankTaskSettings(topN, returnDocuments);
    }

    /**
     * Creates a new {@link OciGenAiRerankTaskSettings} preferring the non-null fields of the request settings over the original
     * settings.
     */
    public static OciGenAiRerankTaskSettings of(
        OciGenAiRerankTaskSettings originalSettings,
        OciGenAiRerankTaskSettings requestTaskSettings
    ) {
        return new OciGenAiRerankTaskSettings(
            requestTaskSettings.topN != null ? requestTaskSettings.topN : originalSettings.topN,
            requestTaskSettings.returnDocuments != null ? requestTaskSettings.returnDocuments : originalSettings.returnDocuments
        );
    }

    private final Integer topN;
    private final Boolean returnDocuments;

    public OciGenAiRerankTaskSettings(@Nullable Integer topN, @Nullable Boolean returnDocuments) {
        this.topN = topN;
        this.returnDocuments = returnDocuments;
    }

    public OciGenAiRerankTaskSettings(StreamInput in) throws IOException {
        this(in.readOptionalVInt(), in.readOptionalBoolean());
    }

    @Override
    @Nullable
    public Integer getTopN() {
        return topN;
    }

    @Nullable
    public Boolean getReturnDocuments() {
        return returnDocuments;
    }

    @Override
    public boolean isEmpty() {
        return topN == null && returnDocuments == null;
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        return of(this, fromMap(newSettings));
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
        return OciGenAiUtils.ML_INFERENCE_OCI_GENAI_ADDED;
    }

    @Override
    public boolean supportsVersion(TransportVersion version) {
        return version.supports(OciGenAiUtils.ML_INFERENCE_OCI_GENAI_ADDED);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalVInt(topN);
        out.writeOptionalBoolean(returnDocuments);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OciGenAiRerankTaskSettings that = (OciGenAiRerankTaskSettings) o;
        return Objects.equals(topN, that.topN) && Objects.equals(returnDocuments, that.returnDocuments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topN, returnDocuments);
    }
}
