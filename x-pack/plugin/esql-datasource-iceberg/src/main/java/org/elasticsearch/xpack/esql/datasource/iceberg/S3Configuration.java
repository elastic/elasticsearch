/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasource.iceberg;

import java.util.Objects;

/**
 * Configuration for S3 access, including credentials and endpoint settings.
 * Built from the settings of an external dataset or data source.
 */
public class S3Configuration {

    private final String accessKey;
    private final String secretKey;
    private final String endpoint;
    private final String region;

    private S3Configuration(String accessKey, String secretKey, String endpoint, String region) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.endpoint = endpoint;
        this.region = region;
    }

    /**
     * Create S3Configuration from individual fields (used for deserialization).
     *
     * @param accessKey access key (nullable)
     * @param secretKey secret key (nullable)
     * @param endpoint endpoint (nullable)
     * @param region region (nullable)
     * @return S3Configuration instance, or null if all fields are null
     */
    public static S3Configuration fromFields(String accessKey, String secretKey, String endpoint, String region) {
        // If no fields are provided, return null (will use default AWS credentials chain)
        if (accessKey == null && secretKey == null && endpoint == null && region == null) {
            return null;
        }
        return new S3Configuration(accessKey, secretKey, endpoint, region);
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
            && Objects.equals(region, that.region);
    }

    @Override
    public int hashCode() {
        return Objects.hash(accessKey, secretKey, endpoint, region);
    }
}
