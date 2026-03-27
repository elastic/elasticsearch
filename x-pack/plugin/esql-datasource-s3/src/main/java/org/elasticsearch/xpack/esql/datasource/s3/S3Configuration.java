/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasource.s3;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.expression.Expression;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Configuration for S3 access including credentials and endpoint settings.
 */
public class S3Configuration {

    private final String accessKey;
    private final String secretKey;
    private final String endpoint;
    private final String region;
    private final String auth;

    private S3Configuration(String accessKey, String secretKey, String endpoint, String region, String auth) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.endpoint = endpoint;
        this.region = region;
        this.auth = auth != null ? auth.toLowerCase(Locale.ROOT) : null;
        validate();
    }

    private void validate() {
        if (auth != null && "none".equals(auth) == false) {
            throw new IllegalArgumentException("Unsupported auth value [" + auth + "]; supported values: [none]");
        }
        if (isAnonymous() && (accessKey != null || secretKey != null)) {
            throw new IllegalArgumentException(
                "auth=none cannot be combined with access_key/secret_key; anonymous access uses no credentials"
            );
        }
    }

    public static S3Configuration fromParams(Map<String, Expression> params) {
        if (params == null || params.isEmpty()) {
            return null;
        }

        String accessKey = extractStringParam(params, "access_key");
        String secretKey = extractStringParam(params, "secret_key");
        String endpoint = extractStringParam(params, "endpoint");
        String region = extractStringParam(params, "region");
        String auth = extractStringParam(params, "auth");

        if (accessKey == null && secretKey == null && endpoint == null && region == null && auth == null) {
            return null;
        }

        return new S3Configuration(accessKey, secretKey, endpoint, region, auth);
    }

    public static S3Configuration fromFields(String accessKey, String secretKey, String endpoint, String region) {
        return fromFields(accessKey, secretKey, endpoint, region, null);
    }

    public static S3Configuration fromFields(String accessKey, String secretKey, String endpoint, String region, String auth) {
        if (accessKey == null && secretKey == null && endpoint == null && region == null && auth == null) {
            return null;
        }
        return new S3Configuration(accessKey, secretKey, endpoint, region, auth);
    }

    private static String extractStringParam(Map<String, Expression> params, String key) {
        Expression expr = params.get(key);
        if (expr instanceof org.elasticsearch.xpack.esql.core.expression.Literal literal) {
            Object value = literal.value();
            if (value instanceof BytesRef bytesRef) {
                return BytesRefs.toString(bytesRef);
            }
            return value != null ? value.toString() : null;
        }
        return null;
    }

    public String accessKey() {
        return accessKey;
    }

    public String secretKey() {
        return secretKey;
    }

    public String endpoint() {
        return endpoint;
    }

    public String region() {
        return region;
    }

    public String auth() {
        return auth;
    }

    public boolean isAnonymous() {
        return "none".equals(auth);
    }

    public boolean hasCredentials() {
        return accessKey != null && secretKey != null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        S3Configuration that = (S3Configuration) o;
        return Objects.equals(accessKey, that.accessKey)
            && Objects.equals(secretKey, that.secretKey)
            && Objects.equals(endpoint, that.endpoint)
            && Objects.equals(region, that.region)
            && Objects.equals(auth, that.auth);
    }

    @Override
    public int hashCode() {
        return Objects.hash(accessKey, secretKey, endpoint, region, auth);
    }
}
