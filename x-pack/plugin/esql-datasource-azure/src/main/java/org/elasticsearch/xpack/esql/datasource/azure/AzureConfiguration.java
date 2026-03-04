/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.azure;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;

import java.util.Map;

/**
 * Configuration for Azure Blob Storage access including credentials and endpoint settings.
 * <p>
 * Supports multiple authentication modes:
 * <ul>
 *   <li>Connection string (full connection string)</li>
 *   <li>Account + key (SharedKey auth)</li>
 *   <li>SAS token</li>
 *   <li>DefaultAzureCredential when no explicit credentials are provided</li>
 * </ul>
 */
public record AzureConfiguration(String connectionString, String account, String key, String sasToken, String endpoint) {

    public static AzureConfiguration fromParams(Map<String, Expression> params) {
        if (params == null || params.isEmpty()) {
            return null;
        }

        String connectionString = extractStringParam(params, "connection_string");
        String account = extractStringParam(params, "account");
        String key = extractStringParam(params, "key");
        String sasToken = extractStringParam(params, "sas_token");
        String endpoint = extractStringParam(params, "endpoint");

        return fromFields(connectionString, account, key, sasToken, endpoint);
    }

    public static AzureConfiguration fromFields(String connectionString, String account, String key, String sasToken, String endpoint) {
        if (connectionString == null && account == null && key == null && sasToken == null && endpoint == null) {
            return null;
        }
        return new AzureConfiguration(connectionString, account, key, sasToken, endpoint);
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
     * Returns true if explicit credentials are provided (connection string, account+key, or SAS token).
     */
    public boolean hasCredentials() {
        return (connectionString != null && connectionString.isEmpty() == false)
            || (account != null && key != null)
            || (sasToken != null && sasToken.isEmpty() == false);
    }
}
