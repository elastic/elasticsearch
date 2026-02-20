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

import java.util.Map;

/**
 * Configuration for Google Cloud Storage access including credentials and endpoint settings.
 * <p>
 * Supports two authentication modes:
 * <ul>
 *   <li>Service account JSON credentials (inline or from a file path)</li>
 *   <li>Application Default Credentials (ADC) when no explicit credentials are provided</li>
 * </ul>
 */
public record GcsConfiguration(String serviceAccountCredentials, String projectId, String endpoint, String tokenUri) {

    public static GcsConfiguration fromParams(Map<String, Expression> params) {
        if (params == null || params.isEmpty()) {
            return null;
        }

        String credentials = extractStringParam(params, "credentials");
        String projectId = extractStringParam(params, "project_id");
        String endpoint = extractStringParam(params, "endpoint");
        String tokenUri = extractStringParam(params, "token_uri");

        return fromFields(credentials, projectId, endpoint, tokenUri);
    }

    public static GcsConfiguration fromFields(String serviceAccountCredentials, String projectId, String endpoint) {
        return fromFields(serviceAccountCredentials, projectId, endpoint, null);
    }

    public static GcsConfiguration fromFields(String serviceAccountCredentials, String projectId, String endpoint, String tokenUri) {
        if (serviceAccountCredentials == null && projectId == null && endpoint == null && tokenUri == null) {
            return null;
        }
        return new GcsConfiguration(serviceAccountCredentials, projectId, endpoint, tokenUri);
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

    /**
     * Returns true if explicit service account credentials are provided.
     */
    public boolean hasCredentials() {
        return serviceAccountCredentials != null;
    }
}
