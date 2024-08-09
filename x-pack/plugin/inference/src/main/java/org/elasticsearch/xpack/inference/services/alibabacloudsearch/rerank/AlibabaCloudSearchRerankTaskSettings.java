/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.rerank;

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
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalBoolean;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;

/**
 * Defines the task settings for the AlibabaCloudSearch rerank service.
 *
 * <p>
 * <a href="https://help.aliyun.com/zh/open-search/search-platform/developer-reference/ranker-api-details">See api docs for details.</a>
 * </p>
 */
public class AlibabaCloudSearchRerankTaskSettings implements TaskSettings {
    public static final String NAME = "alibabacloud_search_rerank_task_settings";
    public static final String RETURN_DOCUMENTS = "return_documents";
    public static final String TOP_N_DOCS_ONLY = "top_n";

    static final AlibabaCloudSearchRerankTaskSettings EMPTY_SETTINGS = new AlibabaCloudSearchRerankTaskSettings(null, null);

    public static AlibabaCloudSearchRerankTaskSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        if (map == null || map.isEmpty()) {
            return EMPTY_SETTINGS;
        }
        Boolean returnDocuments = extractOptionalBoolean(map, RETURN_DOCUMENTS, validationException);
        Integer topNDocumentsOnly = extractOptionalPositiveInteger(
            map,
            TOP_N_DOCS_ONLY,
            ModelConfigurations.TASK_SETTINGS,
            validationException
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return of(topNDocumentsOnly, returnDocuments);
    }

    /**
     * Creates a new {@link AlibabaCloudSearchRerankTaskSettings}
     * by preferring non-null fields from the request settings over the original settings.
     * @param originalSettings    the settings stored as part of the inference entity configuration
     * @param requestTaskSettings the settings passed in within the task_settings field of the request
     * @return a constructed {@link AlibabaCloudSearchRerankTaskSettings}
     */
    public static AlibabaCloudSearchRerankTaskSettings of(
        AlibabaCloudSearchRerankTaskSettings originalSettings,
        AlibabaCloudSearchRerankTaskSettings requestTaskSettings
    ) {
        return new AlibabaCloudSearchRerankTaskSettings(
            requestTaskSettings.getTopNDocumentsOnly() != null
                ? requestTaskSettings.getTopNDocumentsOnly()
                : originalSettings.getTopNDocumentsOnly(),
            requestTaskSettings.getReturnDocuments() != null
                ? requestTaskSettings.getReturnDocuments()
                : originalSettings.getReturnDocuments()
        );
    }

    public static AlibabaCloudSearchRerankTaskSettings of(Integer topNDocumentsOnly, Boolean returnDocuments) {
        return new AlibabaCloudSearchRerankTaskSettings(topNDocumentsOnly, returnDocuments);
    }

    private final Integer topNDocumentsOnly;
    private final Boolean returnDocuments;

    public AlibabaCloudSearchRerankTaskSettings(StreamInput in) throws IOException {
        this(in.readOptionalVInt(), in.readOptionalBoolean());
    }

    public AlibabaCloudSearchRerankTaskSettings(@Nullable Integer topNDocumentsOnly, @Nullable Boolean returnDocuments) {
        this.topNDocumentsOnly = topNDocumentsOnly;
        this.returnDocuments = returnDocuments;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (topNDocumentsOnly != null) {
            builder.field(TOP_N_DOCS_ONLY, topNDocumentsOnly);
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
        return TransportVersions.ML_INFERENCE_ALIBABACLOUD_SEARCH_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalInt(topNDocumentsOnly);
        out.writeOptionalBoolean(returnDocuments);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AlibabaCloudSearchRerankTaskSettings that = (AlibabaCloudSearchRerankTaskSettings) o;
        return Objects.equals(returnDocuments, that.returnDocuments) && Objects.equals(topNDocumentsOnly, that.topNDocumentsOnly);
    }

    @Override
    public int hashCode() {
        return Objects.hash(returnDocuments, topNDocumentsOnly);
    }

    public static String invalidInputTypeMessage(InputType inputType) {
        return Strings.format("received invalid input type value [%s]", inputType.toString());
    }

    // todo : understand this
    public Boolean getDoesReturnDocuments() {
        return returnDocuments;
    }

    public Integer getTopNDocumentsOnly() {
        return topNDocumentsOnly;
    }

    public Boolean getReturnDocuments() {
        return returnDocuments;
    }
}
