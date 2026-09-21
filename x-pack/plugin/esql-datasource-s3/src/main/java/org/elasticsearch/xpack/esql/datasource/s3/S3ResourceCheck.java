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

import java.net.URI;
import java.util.List;
import java.util.Locale;

/**
 * Provider-specific resource validation for S3 URIs, invoked at {@code PUT /_query/dataset} time
 * via {@link org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceValidator#withResourceCheck}.
 *
 * <p>Refuses five forms that pass the scheme check and are not usable: an empty location, a multi-region
 * access point, an ARN, an S3 Express directory bucket, and any other bucket name that steers the request
 * off the regional object endpoint. The last two are here rather than with the endpoint rule because the
 * bucket name alone moves the request, so no endpoint setting can confine it.
 *
 * <p>The ARN branches read the raw string, because {@code StoragePath.of} throws on an ARN authority before
 * any check could run; the SDK's {@code Arn.fromString} is not needed for them. Every branch after that
 * reads {@link StoragePath#host()}, which is the bucket the read itself binds to. Testing the raw authority
 * instead let a {@code :443} suffix walk past all four bucket refusals while the read bound to the bucket
 * they refused, and let a {@code userInfo@} prefix refuse one they admit.
 */
class S3ResourceCheck {

    static final String INCOMPLETE_LOCATION_PREFIX = "[resource] is not a complete object location but was [";
    static final String MRAP_MESSAGE_PREFIX = "[resource] looks like a multi-region access point, which is not supported, but was [";
    static final String ARN_MESSAGE_PREFIX = "[resource] does not accept an ARN but was [";
    static final String ARN_MESSAGE_SUFFIX = "]. Use a bucket name, or an access point alias if the bucket is behind an access point.";
    static final String EXPRESS_MESSAGE_PREFIX = "[resource] looks like an S3 Express directory bucket, which is not supported, but was [";
    static final String STEERED_MESSAGE_PREFIX = "[resource] names a bucket that the AWS SDK routes to [";
    static final String STEERED_MESSAGE_SUFFIX = "], which is not a supported AWS S3 endpoint, but was [";
    static final String UNROUTABLE_MESSAGE_PREFIX = "[resource] names a bucket the AWS SDK cannot route to any endpoint but was [";
    static final String UNPARSEABLE_MESSAGE_PREFIX = "[resource] is not a location this data source can read but was [";

    /**
     * The exact spellings the SDK keys off, both of which build an S3 Express endpoint. It matches the
     * last six characters against {@code --x-s3} and the last seven against {@code --xa-s3}, in separate
     * rules, so one suffix does not cover the other. Matched ahead of the general check below, which
     * would also catch them, so an S3 Express bucket keeps its own message.
     */
    private static final List<String> DIRECTORY_BUCKET_SUFFIXES = List.of("--x-s3", "--xa-s3");

    /** Which region is immaterial: a name that steers the request does so in every region. */
    private static final Region PROBE_REGION = Region.US_EAST_1;

    private S3ResourceCheck() {}

    /**
     * Adds an error per problem found; does not throw. The order is load-bearing twice: an empty authority
     * is refused before format inference reports "cannot determine a format", and an MRAP before the ARN
     * branch can suggest an access point alias, which MRAPs do not have.
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
        String authorityLower = authority.toLowerCase(Locale.ROOT);

        // Only an MRAP ARN needs this: its .mrap marker sits in the path rather than the authority.
        String firstPathSegmentLower = "";
        if (firstSlash >= 0) {
            String afterAuthority = afterScheme.substring(firstSlash + 1);
            int nextSlash = afterAuthority.indexOf('/');
            firstPathSegmentLower = (nextSlash < 0 ? afterAuthority : afterAuthority.substring(0, nextSlash)).toLowerCase(Locale.ROOT);
        }

        // The two ARN forms, refused before the parse below, which throws on an ARN authority. MRAP first,
        // so the generic branch cannot suggest an access point alias, which MRAPs do not have.
        if (authorityLower.startsWith("arn:")) {
            if (firstPathSegmentLower.endsWith(".mrap")) {
                errors.addValidationError(MRAP_MESSAGE_PREFIX + resource + "].");
            } else {
                errors.addValidationError(ARN_MESSAGE_PREFIX + resource + ARN_MESSAGE_SUFFIX);
            }
            return;
        }

        // From here the bucket is whatever the read will bind to, not whatever the authority spells: a port
        // and a userInfo belong to the location, not to the bucket name.
        String bucket;
        try {
            bucket = StoragePath.of(resource).host();
        } catch (IllegalArgumentException e) {
            errors.addValidationError(UNPARSEABLE_MESSAGE_PREFIX + resource + "].");
            return;
        }
        String bucketLower = bucket.toLowerCase(Locale.ROOT);

        if (bucketLower.endsWith(".mrap") || bucketLower.endsWith(".mrap.accesspoint.s3-global.amazonaws.com")) {
            errors.addValidationError(MRAP_MESSAGE_PREFIX + resource + "].");
            return;
        }

        for (String suffix : DIRECTORY_BUCKET_SUFFIXES) {
            if (bucketLower.endsWith(suffix)) {
                errors.addValidationError(EXPRESS_MESSAGE_PREFIX + resource + "].");
                return;
            }
        }

        String bucketHost;
        try {
            bucketHost = resolvedHost(bucket);
        } catch (RuntimeException e) {
            // Thrown for a malformed outpost id. The name reaches no endpoint, so say so rather than
            // admit a resource no read could use.
            errors.addValidationError(UNROUTABLE_MESSAGE_PREFIX + resource + "].");
            return;
        }
        if (S3EndpointCheck.isPermittedHost(bucketHost, S3EndpointCheck.S3_SERVICE) == false) {
            errors.addValidationError(STEERED_MESSAGE_PREFIX + bucketHost + STEERED_MESSAGE_SUFFIX + resource + "].");
        }
    }

    /**
     * The host the SDK builds for this bucket name alone. Asking the resolver rather than listing spellings
     * keeps this from going stale: the SDK's rules are positional, so a name reaches {@code s3-outposts}
     * only once it is long enough to carry an outpost id. Path-style so an ordinary bucket leaves the host
     * bare, which is what {@link S3EndpointCheck#isPermittedHost} accepts; no endpoint, because one does
     * not suppress this steering.
     *
     * @throws RuntimeException if the ruleset cannot resolve the name at all
     */
    private static String resolvedHost(String bucket) {
        URI resolved = S3EndpointProvider.defaultProvider()
            .resolveEndpoint(
                S3EndpointParams.builder()
                    .bucket(bucket)
                    .region(PROBE_REGION)
                    .useFips(false)
                    .useDualStack(false)
                    .accelerate(false)
                    .forcePathStyle(true)
                    .build()
            )
            .join()
            .url();
        return resolved.getHost();
    }
}
