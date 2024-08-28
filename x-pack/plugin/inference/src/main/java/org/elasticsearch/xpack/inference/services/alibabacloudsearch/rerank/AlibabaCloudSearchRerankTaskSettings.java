/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Defines the task settings for the AlibabaCloudSearch rerank service.
 *
 * <p>
 * <a href="https://help.aliyun.com/zh/open-search/search-platform/developer-reference/ranker-api-details">See api docs for details.</a>
 * </p>
 */
public class AlibabaCloudSearchRerankTaskSettings implements TaskSettings {
    public static final String NAME = "alibabacloud_search_rerank_task_settings";

    static final AlibabaCloudSearchRerankTaskSettings EMPTY_SETTINGS = new AlibabaCloudSearchRerankTaskSettings();

    public static AlibabaCloudSearchRerankTaskSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        if (map == null || map.isEmpty()) {
            return EMPTY_SETTINGS;
        }

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return of();
    }

    /**
     * Creates a new {@link AlibabaCloudSearchRerankTaskSettings}
     * by preferring non-null fields from the request settings over the original settings.
     * @return a constructed {@link AlibabaCloudSearchRerankTaskSettings}
     */
    public static AlibabaCloudSearchRerankTaskSettings of(
        AlibabaCloudSearchRerankTaskSettings originalSettings,
        AlibabaCloudSearchRerankTaskSettings requestTaskSettings
    ) {
        return new AlibabaCloudSearchRerankTaskSettings();
    }

    public static AlibabaCloudSearchRerankTaskSettings of() {
        return new AlibabaCloudSearchRerankTaskSettings();
    }

    public AlibabaCloudSearchRerankTaskSettings(StreamInput in) {
        this();
    }

    public AlibabaCloudSearchRerankTaskSettings() {}

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
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
    public void writeTo(StreamOutput out) throws IOException {}

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash();
    }
}
