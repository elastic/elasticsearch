/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.s3;

import com.amazonaws.auth.AWSSessionCredentials;

import java.util.Objects;

final class S3BasicSessionCredentials extends S3BasicCredentials implements AWSSessionCredentials {

    private final String sessionToken;

    S3BasicSessionCredentials(String accessKey, String secretKey, String sessionToken) {
        super(accessKey, secretKey);
        this.sessionToken = sessionToken;
    }

    @Override
    public String getSessionToken() {
        return sessionToken;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final S3BasicSessionCredentials that = (S3BasicSessionCredentials) o;
        return sessionToken.equals(that.sessionToken)
            && getAWSAccessKeyId().equals(that.getAWSAccessKeyId())
            && getAWSSecretKey().equals(that.getAWSSecretKey());
    }

    @Override
    public int hashCode() {
        return Objects.hash(sessionToken, getAWSAccessKeyId(), getAWSSecretKey());
    }
}
