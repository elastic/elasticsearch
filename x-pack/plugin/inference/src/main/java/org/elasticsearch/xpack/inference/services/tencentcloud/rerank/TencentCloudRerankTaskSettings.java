/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TopNProvider;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalBoolean;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;

/**
 * Task settings for TencentCloud rerank: {@code top_n} and {@code return_documents}.
 */
public class TencentCloudRerankTaskSettings implements TaskSettings, TopNProvider {

    public static final String NAME = "tencentcloud_rerank_task_settings";
    public static final String RETURN_DOCUMENTS = "return_documents";
    public static final String TOP_N = "top_n";

    public static final TencentCloudRerankTaskSettings EMPTY_SETTINGS = new TencentCloudRerankTaskSettings(null, null);

    public static TencentCloudRerankTaskSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        if (map == null || map.isEmpty()) {
            return EMPTY_SETTINGS;
        }

        Boolean returnDocuments = extractOptionalBoolean(map, RETURN_DOCUMENTS, validationException);
        Integer topN = extractOptionalPositiveInteger(map, TOP_N, ModelConfigurations.TASK_SETTINGS, validationException);

        validationException.throwIfValidationErrorsExist();

        if (returnDocuments == null && topN == null) {
            return EMPTY_SETTINGS;
        }

        return new TencentCloudRerankTaskSettings(topN, returnDocuments);
    }

    /**
     * Merge task settings, preferring non-null fields from {@code requestTaskSettings}.
     */
    public static TencentCloudRerankTaskSettings of(
        TencentCloudRerankTaskSettings originalSettings,
        TencentCloudRerankTaskSettings requestTaskSettings
    ) {
        return new TencentCloudRerankTaskSettings(
            requestTaskSettings.getTopN() != null ? requestTaskSettings.getTopN() : originalSettings.getTopN(),
            requestTaskSettings.getReturnDocuments() != null
                ? requestTaskSettings.getReturnDocuments()
                : originalSettings.getReturnDocuments()
        );
    }

    private final Integer topN;
    private final Boolean returnDocuments;

    public TencentCloudRerankTaskSettings(@Nullable Integer topN, @Nullable Boolean returnDocuments) {
        this.topN = topN;
        this.returnDocuments = returnDocuments;
    }

    public TencentCloudRerankTaskSettings(StreamInput in) throws IOException {
        this(in.readOptionalInt(), in.readOptionalBoolean());
    }

    @Override
    public boolean isEmpty() {
        return topN == null && returnDocuments == null;
    }

    @Override
    public Integer getTopN() {
        return topN;
    }

    public Boolean getReturnDocuments() {
        return returnDocuments;
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
        return TransportVersion.minimumCompatible();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalInt(topN);
        out.writeOptionalBoolean(returnDocuments);
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        return of(this, TencentCloudRerankTaskSettings.fromMap(newSettings));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TencentCloudRerankTaskSettings that = (TencentCloudRerankTaskSettings) o;
        return Objects.equals(topN, that.topN) && Objects.equals(returnDocuments, that.returnDocuments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topN, returnDocuments);
    }
}
