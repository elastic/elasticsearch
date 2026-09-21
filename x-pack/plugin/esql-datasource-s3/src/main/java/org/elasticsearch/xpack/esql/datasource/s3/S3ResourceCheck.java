/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.endpoints.S3EndpointParams;
import software.amazon.awssdk.services.s3.endpoints.S3EndpointProvider;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletionException;

/**
 * Provider-specific resource validation for S3 URIs, invoked at {@code PUT /_query/dataset} time
 * via {@link org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceValidator#withResourceCheck}.
 *
 * <p>Refuses an empty location, an ARN, a multi-region access point, and any bucket name that routes off the
 * regional object endpoint on its own — which no {@code endpoint} setting can prevent. ARNs are checked on the
 * raw string because {@code StoragePath.of} throws on them; everything else checks {@link StoragePath#host()},
 * the bucket the read binds to, so a port or {@code userInfo} cannot make the check and the read disagree.
 */
class S3ResourceCheck {

    static final String INCOMPLETE_LOCATION_PREFIX = "[resource] is not a complete object location but was [";
    static final String MRAP_MESSAGE_PREFIX = "[resource] looks like a multi-region access point, which is not supported, but was [";
    static final String ARN_MESSAGE_PREFIX = "[resource] does not accept an ARN but was [";
    static final String ARN_MESSAGE_SUFFIX = "]. Use a bucket name, or an access point alias if the bucket is behind an access point.";

    private S3ResourceCheck() {}

    /**
     * Adds an error per problem found; does not throw. An empty location is refused before format inference can
     * misreport it, and an MRAP ARN before the generic ARN branch can suggest an access point alias it lacks.
     */
    static void validate(String resource, ValidationException errors) {
        int schemeEnd = resource.indexOf("://");
        if (schemeEnd < 0) {
            return;
        }
        String afterScheme = resource.substring(schemeEnd + 3);
        int firstSlash = afterScheme.indexOf('/');
        String authority = firstSlash < 0 ? afterScheme : afterScheme.substring(0, firstSlash);
        if (authority.isEmpty()) {
            errors.addValidationError(INCOMPLETE_LOCATION_PREFIX + resource + "].");
            return;
        }

        if (authority.toLowerCase(Locale.ROOT).startsWith("arn:")) {
            // An MRAP ARN carries its .mrap marker in the first path segment rather than the authority.
            String path = firstSlash < 0 ? "" : afterScheme.substring(firstSlash + 1);
            int nextSlash = path.indexOf('/');
            String firstSegment = (nextSlash < 0 ? path : path.substring(0, nextSlash)).toLowerCase(Locale.ROOT);
            if (firstSegment.endsWith(".mrap")) {
                errors.addValidationError(MRAP_MESSAGE_PREFIX + resource + "].");
            } else {
                errors.addValidationError(ARN_MESSAGE_PREFIX + resource + ARN_MESSAGE_SUFFIX);
            }
            return;
        }

        String bucket;
        try {
            bucket = StoragePath.of(resource).host();
        } catch (IllegalArgumentException e) {
            errors.addValidationError("[resource] is not a location this data source can read but was [" + resource + "].");
            return;
        }
        String bucketLower = bucket.toLowerCase(Locale.ROOT);

        if (bucketLower.endsWith(".mrap") || bucketLower.endsWith(".mrap.accesspoint.s3-global.amazonaws.com")) {
            errors.addValidationError(MRAP_MESSAGE_PREFIX + resource + "].");
            return;
        }
        // Caught by the resolver check below too; matched first so a directory bucket gets a message naming it.
        for (String suffix : List.of("--x-s3", "--xa-s3")) {
            if (bucketLower.endsWith(suffix)) {
                errors.addValidationError(
                    "[resource] looks like an S3 Express directory bucket, which is not supported, but was [" + resource + "]."
                );
                return;
            }
        }

        String host;
        try {
            host = resolvedHost(bucket);
        } catch (CompletionException e) {
            // The ruleset rejects some names outright, such as one carrying a malformed outpost id.
            errors.addValidationError("[resource] names a bucket the AWS SDK cannot route to any endpoint but was [" + resource + "].");
            return;
        }
        if (S3EndpointCheck.isPermittedHost(host, S3EndpointCheck.S3_SERVICE) == false) {
            errors.addValidationError(
                "[resource] names a bucket that the AWS SDK routes to ["
                    + host
                    + "], which is not a supported AWS S3 endpoint, but was ["
                    + resource
                    + "]."
            );
        }
    }

    /**
     * The host the SDK builds for this bucket name alone. Asking the resolver rather than listing spellings keeps
     * this current: its rules are positional, so a {@code --op-s3} name reaches {@code s3-outposts} only once it is
     * long enough to carry an outpost id. Path-style leaves an ordinary bucket's host bare; no endpoint is given,
     * because one does not suppress this. Any region will do: a name that steers does so in all of them.
     */
    private static String resolvedHost(String bucket) {
        return S3EndpointProvider.defaultProvider()
            .resolveEndpoint(
                S3EndpointParams.builder()
                    .bucket(bucket)
                    .region(Region.US_EAST_1)
                    .useFips(false)
                    .useDualStack(false)
                    .accelerate(false)
                    .forcePathStyle(true)
                    .build()
            )
            .join()
            .url()
            .getHost();
    }
}
