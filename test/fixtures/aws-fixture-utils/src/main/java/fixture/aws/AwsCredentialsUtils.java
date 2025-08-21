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
     * Region supplier which matches any region.
     */
    // TODO: replace with DynamicRegionSupplier.
    public static final Supplier<String> ANY_REGION = () -> "*";

    /**
     * @return an authorization predicate that ensures the authorization header matches the given access key, region and service name.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-auth-using-authorization-header.html">AWS v4 Signatures</a>
     * @param regionSupplier supplies the name of the AWS region used to sign the request, or {@code *} to skip validation of the region
     *                       parameter
     */
    public static BiPredicate<String, String> fixedAccessKey(String accessKey, Supplier<String> regionSupplier, String serviceName) {
        return mutableAccessKey(() -> accessKey, regionSupplier, serviceName);
    }

    /**
     * @return an authorization predicate that ensures the authorization header matches the access key supplied by the given supplier,
     *         and also matches the given region and service name.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-auth-using-authorization-header.html">AWS v4 Signatures</a>
     * @param regionSupplier supplies the name of the AWS region used to sign the request, or {@code *} to skip validation of the region
     *                       parameter
     */
    public static BiPredicate<String, String> mutableAccessKey(
        Supplier<String> accessKeySupplier,
        Supplier<String> regionSupplier,
        String serviceName
    ) {
        return (authorizationHeader, sessionTokenHeader) -> authorizationHeader != null
            && isValidAwsV4SignedAuthorizationHeader(accessKeySupplier.get(), regionSupplier.get(), serviceName, authorizationHeader);
    }

    /**
     * @return whether the given value is a valid AWS-v4-signed authorization header that matches the given access key, region, and service
     * name.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-auth-using-authorization-header.html">AWS v4 Signatures</a>
     * @param region the name of the AWS region used to sign the request, or {@code *} to skip validation of the region parameter
     */
    public static boolean isValidAwsV4SignedAuthorizationHeader(
        String accessKey,
        String region,
        String serviceName,
        String authorizationHeader
    ) {
        final var expectedPrefix = "AWS4-HMAC-SHA256 Credential=" + accessKey + "/";
        if (authorizationHeader.startsWith(expectedPrefix) == false) {
            return false;
        }

        if (region.equals("*")) {
            // skip region validation; TODO eliminate this when region is fixed in all tests
            return authorizationHeader.contains("/" + serviceName + "/aws4_request, ");
        }

        final var remainder = authorizationHeader.substring(expectedPrefix.length() + "YYYYMMDD".length() /* skip over date field */);
        return remainder.startsWith("/" + region + "/" + serviceName + "/aws4_request, ");
    }

    /**
     * @return an authorization predicate that ensures the access key, session token, region and service name all match the given values.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-auth-using-authorization-header.html">AWS v4 Signatures</a>
     * @param regionSupplier supplies the name of the AWS region used to sign the request, or {@code *} to skip validation of the region
     *                       parameter
     */
    public static BiPredicate<String, String> fixedAccessKeyAndToken(
        String accessKey,
        String sessionToken,
        Supplier<String> regionSupplier,
        String serviceName
    ) {
        Objects.requireNonNull(sessionToken);
        final var accessKeyPredicate = fixedAccessKey(accessKey, regionSupplier, serviceName);
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
