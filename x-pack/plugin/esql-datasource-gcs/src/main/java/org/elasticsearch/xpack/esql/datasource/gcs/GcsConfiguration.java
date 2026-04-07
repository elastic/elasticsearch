/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasource.gcs;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;

import java.util.Locale;
import java.util.Map;

/**
 * Configuration for Google Cloud Storage access including credentials and endpoint settings.
 * <p>
 * Supports authentication modes:
 * <ul>
 *   <li>Service account JSON credentials (inline or from a file path)</li>
 *   <li>{@code auth=none} for anonymous access to public buckets</li>
 *   <li>Application Default Credentials (ADC) when no explicit credentials are provided</li>
 * </ul>
 */
public record GcsConfiguration(String serviceAccountCredentials, String projectId, String endpoint, String tokenUri, String auth) {

    public GcsConfiguration {
        auth = auth != null ? auth.toLowerCase(Locale.ROOT) : null;
        if (auth != null && "none".equals(auth) == false) {
            throw new IllegalArgumentException("Unsupported auth value [" + auth + "]; supported values: [none]");
        }
        if ("none".equals(auth) && serviceAccountCredentials != null) {
            throw new IllegalArgumentException("auth=none cannot be combined with credentials; anonymous access uses no credentials");
        }
    }

    public static GcsConfiguration fromParams(Map<String, Expression> params) {
        if (params == null || params.isEmpty()) {
            return null;
        }

        String credentials = extractStringParam(params, "credentials");
        String projectId = extractStringParam(params, "project_id");
        String endpoint = extractStringParam(params, "endpoint");
        String tokenUri = extractStringParam(params, "token_uri");
        String auth = extractStringParam(params, "auth");

        return fromFields(credentials, projectId, endpoint, tokenUri, auth);
    }

    public static GcsConfiguration fromFields(String serviceAccountCredentials, String projectId, String endpoint) {
        return fromFields(serviceAccountCredentials, projectId, endpoint, null, null);
    }

    public static GcsConfiguration fromFields(String serviceAccountCredentials, String projectId, String endpoint, String tokenUri) {
        return fromFields(serviceAccountCredentials, projectId, endpoint, tokenUri, null);
    }

    public static GcsConfiguration fromFields(
        String serviceAccountCredentials,
        String projectId,
        String endpoint,
        String tokenUri,
        String auth
    ) {
        if (serviceAccountCredentials == null && projectId == null && endpoint == null && tokenUri == null && auth == null) {
            return null;
        }
        return new GcsConfiguration(serviceAccountCredentials, projectId, endpoint, tokenUri, auth);
    }

    private static String extractStringParam(Map<String, Expression> params, String key) {
        Expression expr = params.get(key);
        if (expr instanceof Literal literal) {
            Object value = literal.value();
            if (value instanceof BytesRef bytesRef) {
                return BytesRefs.toString(bytesRef);
            }
            return value != null ? value.toString() : null;
        }
        return null;
    }

    public boolean isAnonymous() {
        return "none".equals(auth);
    }

    public boolean hasCredentials() {
        return serviceAccountCredentials != null;
    }
}
