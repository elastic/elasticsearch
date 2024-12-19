/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package fixture.s3;

import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Allows dynamic creation of access-key/session-token credentials for accessing AWS services such as S3. Typically there's one service
 * (e.g. IMDS or STS) which creates credentials dynamically and registers them here using {@link #addValidCredentials}, and then the
 * {@link S3HttpFixture} uses {@link #isAuthorized} to validate the credentials it receives corresponds with some previously-generated
 * credentials.
 */
public class DynamicS3Credentials {
    private final Map<String, Set<String>> validCredentialsMap = ConcurrentCollections.newConcurrentMap();

    public boolean isAuthorized(String authorizationHeader, String sessionTokenHeader) {
        return authorizationHeader != null
            && sessionTokenHeader != null
            && validCredentialsMap.getOrDefault(sessionTokenHeader, Set.of()).stream().anyMatch(authorizationHeader::contains);
    }

    public void addValidCredentials(String accessKey, String sessionToken) {
        validCredentialsMap.computeIfAbsent(
            Objects.requireNonNull(sessionToken, "sessionToken"),
            t -> ConcurrentCollections.newConcurrentSet()
        ).add(Objects.requireNonNull(accessKey, "accessKey"));
    }
}
