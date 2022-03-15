/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery.ec2;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.SuppressForbidden;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

class AwsEc2Utils {

    private static final Logger logger = LogManager.getLogger(AwsEc2Utils.class);
    private static final int CONNECT_TIMEOUT = 2000;
    private static final int METADATA_TOKEN_TTL_SECONDS = 10;
    static final String X_AWS_EC_2_METADATA_TOKEN = "X-aws-ec2-metadata-token";

    @SuppressForbidden(reason = "We call getInputStream in doPrivileged and provide SocketPermission")
    static Optional<String> getMetadataToken(String metadataTokenUrl) {
        if (Strings.isNullOrEmpty(metadataTokenUrl)) {
            return Optional.empty();
        }
        // Gets a new IMDSv2 token https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configuring-instance-metadata-service.html
        return SocketAccess.doPrivileged(() -> {
            HttpURLConnection urlConnection;
            try {
                urlConnection = (HttpURLConnection) new URL(metadataTokenUrl).openConnection();
                urlConnection.setRequestMethod("PUT");
                urlConnection.setConnectTimeout(CONNECT_TIMEOUT);
                urlConnection.setRequestProperty("X-aws-ec2-metadata-token-ttl-seconds", String.valueOf(METADATA_TOKEN_TTL_SECONDS));
            } catch (IOException e) {
                logger.warn("Unable to access the IMDSv2 URI: " + metadataTokenUrl, e);
                return Optional.empty();
            }
            try (
                var in = urlConnection.getInputStream();
                var reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))
            ) {
                return Optional.ofNullable(reader.readLine()).filter(s -> s.isBlank() == false);
            } catch (IOException e) {
                logger.warn("Unable to get a session token from IMDSv2 URI: " + metadataTokenUrl, e);
                return Optional.empty();
            }
        });
    }
}
