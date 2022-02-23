/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.s3;

import com.amazonaws.auth.AWSCredentials;

import java.util.Objects;

class S3BasicCredentials implements AWSCredentials {

    private final String accessKey;

    private final String secretKey;

    S3BasicCredentials(String accessKey, String secretKey) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
    }

    @Override
    public final String getAWSAccessKeyId() {
        return accessKey;
    }

    @Override
    public final String getAWSSecretKey() {
        return secretKey;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        return o instanceof S3BasicCredentials that && accessKey.equals(that.accessKey) && secretKey.equals(that.secretKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(accessKey, secretKey);
    }
}
