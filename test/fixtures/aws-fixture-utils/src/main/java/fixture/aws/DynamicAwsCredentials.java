/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package fixture.aws;

import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Allows dynamic creation of access-key/session-token credentials for accessing AWS services such as S3. Typically there's one service
 * (e.g. IMDS or STS) which creates credentials dynamically and registers them here using {@link #addValidCredentials}, and then the
 * fixture uses {@link #isAuthorized} to validate the credentials it receives corresponds with some previously-generated credentials.
 */
public class DynamicAwsCredentials {

    /**
     * Extra validation that requests are signed using the correct region. Lazy so it can be randomly generated after initialization, since
     * randomness is not available in static context.
     */
    private final Supplier<String> expectedRegionSupplier;

    /**
     * Extra validation that requests are directed to the correct service.
     */
    private final String expectedServiceName;

    /**
     * The set of access keys for each session token registered with {@link #addValidCredentials}. It's this way round because the session
     * token is a separate header so it's easier to extract.
     */
    private final Map<String, Set<String>> validCredentialsMap = ConcurrentCollections.newConcurrentMap();

    /**
     * @param expectedRegion The region to use for validating the authorization header, or {@code *} to skip this validation.
     * @param expectedServiceName The service name that should appear in the authorization header.
     */
    public DynamicAwsCredentials(String expectedRegion, String expectedServiceName) {
        this(() -> expectedRegion, expectedServiceName);
    }

    /**
     * @param expectedRegionSupplier Supplies the region to use for validating the authorization header, or {@code *} to skip this
     *                               validation.
     * @param expectedServiceName The service name that should appear in the authorization header.
     */
    public DynamicAwsCredentials(Supplier<String> expectedRegionSupplier, String expectedServiceName) {
        this.expectedRegionSupplier = expectedRegionSupplier;
        this.expectedServiceName = expectedServiceName;
    }

    public boolean isAuthorized(String authorizationHeader, String sessionTokenHeader) {
        return authorizationHeader != null
            && sessionTokenHeader != null
            && validCredentialsMap.getOrDefault(sessionTokenHeader, Set.of())
                .stream()
                .anyMatch(
                    validAccessKey -> AwsCredentialsUtils.isValidAwsV4SignedAuthorizationHeader(
                        validAccessKey,
                        expectedRegionSupplier.get(),
                        expectedServiceName,
                        authorizationHeader
                    )
                );
    }

    public void addValidCredentials(String accessKey, String sessionToken) {
        validCredentialsMap.computeIfAbsent(
            Objects.requireNonNull(sessionToken, "sessionToken"),
            t -> ConcurrentCollections.newConcurrentSet()
        ).add(Objects.requireNonNull(accessKey, "accessKey"));
    }
}
