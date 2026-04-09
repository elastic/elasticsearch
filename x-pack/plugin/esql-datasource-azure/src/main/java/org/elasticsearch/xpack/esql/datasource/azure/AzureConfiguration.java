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

import java.util.Locale;
import java.util.Map;

/**
 * Configuration for Azure Blob Storage access including credentials and endpoint settings.
 * <p>
 * Supports multiple authentication modes:
 * <ul>
 *   <li>Connection string (full connection string)</li>
 *   <li>Account + key (SharedKey auth)</li>
 *   <li>SAS token</li>
 *   <li>{@code auth=none} for anonymous access to public containers</li>
 *   <li>DefaultAzureCredential when no explicit credentials are provided</li>
 * </ul>
 */
public record AzureConfiguration(String connectionString, String account, String key, String sasToken, String endpoint, String auth) {

    public AzureConfiguration {
        auth = auth != null ? auth.toLowerCase(Locale.ROOT) : null;
        if (auth != null && "none".equals(auth) == false) {
            throw new IllegalArgumentException("Unsupported auth value [" + auth + "]; supported values: [none]");
        }
        if ("none".equals(auth) && hasExplicitCredentials(connectionString, account, key, sasToken)) {
            throw new IllegalArgumentException(
                "auth=none cannot be combined with connection_string/account+key/sas_token; anonymous access uses no credentials"
            );
        }
    }

    public static AzureConfiguration fromParams(Map<String, Expression> params) {
        if (params == null || params.isEmpty()) {
            return null;
        }

        String connectionString = extractStringParam(params, "connection_string");
        String account = extractStringParam(params, "account");
        String key = extractStringParam(params, "key");
        String sasToken = extractStringParam(params, "sas_token");
        String endpoint = extractStringParam(params, "endpoint");
        String auth = extractStringParam(params, "auth");

        return fromFields(connectionString, account, key, sasToken, endpoint, auth);
    }

    public static AzureConfiguration fromFields(String connectionString, String account, String key, String sasToken, String endpoint) {
        return fromFields(connectionString, account, key, sasToken, endpoint, null);
    }

    public static AzureConfiguration fromFields(
        String connectionString,
        String account,
        String key,
        String sasToken,
        String endpoint,
        String auth
    ) {
        if (connectionString == null && account == null && key == null && sasToken == null && endpoint == null && auth == null) {
            return null;
        }
        return new AzureConfiguration(connectionString, account, key, sasToken, endpoint, auth);
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
        return hasExplicitCredentials(connectionString, account, key, sasToken);
    }

    private static boolean hasExplicitCredentials(String connectionString, String account, String key, String sasToken) {
        return (connectionString != null && connectionString.isEmpty() == false)
            || (account != null && key != null)
            || (sasToken != null && sasToken.isEmpty() == false);
    }
}
