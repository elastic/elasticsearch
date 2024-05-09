/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;

import java.util.Locale;
import java.util.Objects;

public enum AzureAiStudioEndpointType {
    TOKEN,
    REALTIME;

    public static String NAME = "azure_ai_studio_endpoint_type";

    public static AzureAiStudioEndpointType fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    public static AzureAiStudioEndpointType fromStringOrStatusException(String name) {
        try {
            AzureAiStudioEndpointType endpointType = AzureAiStudioEndpointType.fromString(name);
            return Objects.requireNonNull(endpointType);
        } catch (IllegalArgumentException e) {
            throw new ElasticsearchStatusException("Unknown azure_ai_studio_endpoint_type [{}]", RestStatus.BAD_REQUEST, name);
        }
    }

    public static String unsupportedAzureAiStudioProviderErrorMsg(String endpointType, String serviceName) {
        return "The [" + serviceName + "] service does not support the endpoint type [" + endpointType + "]";
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
