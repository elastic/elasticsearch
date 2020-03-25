/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final S3BasicCredentials that = (S3BasicCredentials) o;
        return accessKey.equals(that.accessKey) && secretKey.equals(that.secretKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(accessKey, secretKey);
    }
}
