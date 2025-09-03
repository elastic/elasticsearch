/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.discovery.ec2;

import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.imds.Ec2MetadataClient;

import org.elasticsearch.common.Strings;

import java.time.Duration;

class AwsEc2Utils {
    private static final Duration IMDS_CONNECTION_TIMEOUT = Duration.ofSeconds(2);

    static String getInstanceMetadata(String metadataPath) {
        final var httpClientBuilder = ApacheHttpClient.builder();
        httpClientBuilder.connectionTimeout(IMDS_CONNECTION_TIMEOUT);
        try (var ec2Client = Ec2MetadataClient.builder().httpClient(httpClientBuilder).build()) {
            final var metadataValue = ec2Client.get(metadataPath).asString();
            if (Strings.hasText(metadataValue) == false) {
                throw new IllegalStateException("no ec2 metadata returned from " + metadataPath);
            }
            return metadataValue;
        }
    }
}
