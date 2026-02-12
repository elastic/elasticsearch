/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.mixedbread.MixedbreadUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalBoolean;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;

public class MixedbreadRerankTaskSettings implements TaskSettings {
    public static final String NAME = "mixedbread_rerank_task_settings";
    public static final String RETURN_DOCUMENTS = "return_documents";
    public static final String TOP_N = "top_n";

    public static final MixedbreadRerankTaskSettings EMPTY_SETTINGS = new MixedbreadRerankTaskSettings(null, null);

    public static MixedbreadRerankTaskSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        if (map == null || map.isEmpty()) {
            return EMPTY_SETTINGS;
        }

        Boolean returnDocuments = extractOptionalBoolean(map, RETURN_DOCUMENTS, validationException);
        Integer topN = extractOptionalPositiveInteger(map, TOP_N, ModelConfigurations.TASK_SETTINGS, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        if (returnDocuments == null && topN == null) {
            return EMPTY_SETTINGS;
        }

        return new MixedbreadRerankTaskSettings(topN, returnDocuments);
    }

    /**
     * Creates a new {@link MixedbreadRerankTaskSettings}
     * by preferring non-null fields from the request settings over the original settings.
     *
     * @param originalSettings    the settings stored as part of the inference entity configuration
     * @param requestTaskSettings the settings passed in within the task_settings field of the request
     * @return a constructed {@link MixedbreadRerankTaskSettings}
     */
    public static MixedbreadRerankTaskSettings of(
        MixedbreadRerankTaskSettings originalSettings,
        MixedbreadRerankTaskSettings requestTaskSettings
    ) {
        if (requestTaskSettings.isEmpty() || originalSettings.equals(requestTaskSettings)) {
            return originalSettings;
        }
        return new MixedbreadRerankTaskSettings(
            requestTaskSettings.getTopN() != null ? requestTaskSettings.getTopN() : originalSettings.getTopN(),
            requestTaskSettings.getReturnDocuments() != null
                ? requestTaskSettings.getReturnDocuments()
                : originalSettings.getReturnDocuments()
        );
    }

    private final Integer topN;
    private final Boolean returnDocuments;

    public MixedbreadRerankTaskSettings(StreamInput in) throws IOException {
        this(in.readOptionalVInt(), in.readOptionalBoolean());
    }

    public MixedbreadRerankTaskSettings(@Nullable Integer topN, @Nullable Boolean doReturnDocuments) {
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
        return MixedbreadUtils.INFERENCE_MIXEDBREAD_ADDED;
    }

    @Override
    public boolean supportsVersion(TransportVersion version) {
        return MixedbreadUtils.supportsMixedbread(version);
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
        MixedbreadRerankTaskSettings that = (MixedbreadRerankTaskSettings) o;
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
    public MixedbreadRerankTaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        MixedbreadRerankTaskSettings updatedSettings = MixedbreadRerankTaskSettings.fromMap(new HashMap<>(newSettings));
        return MixedbreadRerankTaskSettings.of(this, updatedSettings);
    }
}
