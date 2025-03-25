/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package fixture.aws;

import com.sun.net.httpserver.HttpExchange;

import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.Supplier;

import static fixture.aws.AwsFixtureUtils.sendError;

public enum AwsCredentialsUtils {
    ;

    /**
     * @return an authorization predicate that ensures the access key matches the given values.
     */
    public static BiPredicate<String, String> fixedAccessKey(String accessKey, String region, String service) {
        return mutableAccessKey(() -> accessKey, region, service);
    }

    /**
     * @return an authorization predicate that ensures the authorization header matches the access key supplied by the given supplier,
     *         and the given region and service name.
     */
    public static BiPredicate<String, String> mutableAccessKey(Supplier<String> accessKeySupplier, String region, String service) {
        return (authorizationHeader, sessionTokenHeader) -> {
            if (authorizationHeader == null) {
                return false;
            }

            final var accessKey = accessKeySupplier.get();
            final var expectedPrefix = "AWS4-HMAC-SHA256 Credential=" + accessKey + "/";
            if (authorizationHeader.startsWith(expectedPrefix) == false) {
                return false;
            }

            if (region.equals("*")) {
                // skip region validation; TODO eliminate this when region is fixed in all tests
                return authorizationHeader.contains("/" + service + "/aws4_request, ");
            }

            final var remainder = authorizationHeader.substring(expectedPrefix.length() + 8 /* YYYYMMDD not validated */);
            return remainder.startsWith("/" + region + "/" + service + "/aws4_request, ");
        };
    }

    /**
     * @return an authorization predicate that ensures the access key and session token both match the given values.
     */
    public static BiPredicate<String, String> fixedAccessKeyAndToken(String accessKey, String sessionToken, String region, String service) {
        Objects.requireNonNull(sessionToken);
        final var accessKeyPredicate = fixedAccessKey(accessKey, region, service);
        return (authorizationHeader, sessionTokenHeader) -> accessKeyPredicate.test(authorizationHeader, sessionTokenHeader)
            && sessionToken.equals(sessionTokenHeader);
    }

    /**
     * Check the authorization headers of the given {@param exchange} against the given {@param authorizationPredicate}. If they match,
     * returns {@code true}. If they do not match, sends a {@code 403 Forbidden} response and returns {@code false}.
     */
    public static boolean checkAuthorization(BiPredicate<String, String> authorizationPredicate, HttpExchange exchange) throws IOException {
        if (authorizationPredicate.test(
            exchange.getRequestHeaders().getFirst("Authorization"),
            exchange.getRequestHeaders().getFirst("x-amz-security-token")
        )) {
            return true;
        }

        sendError(exchange, RestStatus.FORBIDDEN, "AccessDenied", "Access denied by " + authorizationPredicate);
        return false;
    }
}
