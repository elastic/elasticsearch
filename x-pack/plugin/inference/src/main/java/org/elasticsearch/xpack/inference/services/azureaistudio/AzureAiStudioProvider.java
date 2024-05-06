/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

public enum AzureAiStudioProvider implements Writeable {
    OPENAI,
    MISTRAL,
    META,
    MICROSOFT_PHI,
    COHERE,
    SNOWFLAKE,
    DATABRICKS;

    public static String NAME = "azure_ai_studio_provider";

    public static AzureAiStudioProvider fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    public static AzureAiStudioProvider fromStringOrStatusException(String name) {
        try {
            AzureAiStudioProvider provider = AzureAiStudioProvider.fromString(name);
            return Objects.requireNonNull(provider);
        } catch (IllegalArgumentException e) {
            throw new ElasticsearchStatusException("Unknown azure_ai_studio_provider [{}]", RestStatus.BAD_REQUEST, name);
        }
    }

    public static String unsupportedAzureAiStudioProviderErrorMsg(String provider, String serviceName) {
        return "The [" + serviceName + "] service does not support the provider [" + provider + "]";
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(this);
    }
}
