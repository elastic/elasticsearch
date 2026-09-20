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

import java.net.URI;
import java.util.List;
import java.util.Locale;

/**
 * Provider-specific resource validation for S3 URIs, invoked at {@code PUT /_query/dataset} time
 * via {@link org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceValidator#withResourceCheck}.
 *
 * <p>Refuses five forms that pass the scheme check and are not usable: an empty location, a
 * multi-region access point, an ARN, an S3 Express directory bucket, and any other bucket name that
 * steers the request off the regional object endpoint. The last two are here rather than with the
 * endpoint rule because the bucket name alone moves the request, so no endpoint setting can confine it.
 *
 * <p>Parsing is on the raw string: {@code StoragePath.of} throws {@code Malformed authority in location}
 * on an ARN before any check could run. The SDK's {@code Arn.fromString} is also not used — the string
 * checks below cover all cases.
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

    /**
     * The exact spellings the SDK keys off, both of which build an S3 Express endpoint. It matches the
     * last six characters against {@code --x-s3} and the last seven against {@code --xa-s3}, in separate
     * rules, so one suffix does not cover the other. They are listed here, ahead of the general check
     * below that would also catch them, so that an S3 Express bucket keeps its own message.
     */
    private static final List<String> DIRECTORY_BUCKET_SUFFIXES = List.of("--x-s3", "--xa-s3");

    /**
     * The region the general check below resolves against. Which region is immaterial — a bucket name
     * that steers the request carries its own host in every region, and one that does not resolves to
     * that region's plain object host — so a fixed value keeps the check independent of the settings,
     * which do not reach it.
     */
    private static final Region PROBE_REGION = Region.US_EAST_1;

    private S3ResourceCheck() {}

    /**
     * Adds an error per problem found; does not throw. The order is load-bearing twice over: an empty
     * authority is refused before format inference can report it as "cannot determine a format", and an
     * MRAP is refused before the generic ARN branch can suggest an access point alias, which MRAPs do
     * not have.
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

        if (authorityLower.endsWith(".mrap")
            || authorityLower.endsWith(".mrap.accesspoint.s3-global.amazonaws.com")
            || (authorityLower.startsWith("arn:") && firstPathSegmentLower.endsWith(".mrap"))) {
            errors.addValidationError(MRAP_MESSAGE_PREFIX + resource + "].");
            return;
        }

        for (String suffix : DIRECTORY_BUCKET_SUFFIXES) {
            if (authorityLower.endsWith(suffix)) {
                errors.addValidationError(EXPRESS_MESSAGE_PREFIX + resource + "].");
                return;
            }
        }

        if (authorityLower.startsWith("arn:")) {
            errors.addValidationError(ARN_MESSAGE_PREFIX + resource + ARN_MESSAGE_SUFFIX);
            return;
        }

        String bucketHost;
        try {
            bucketHost = resolvedHost(authority);
        } catch (RuntimeException e) {
            // The ruleset threw rather than resolving, which it does for a name carrying a malformed
            // outpost id. Such a name reaches no endpoint at all, so report that rather than admitting
            // a resource no read could ever use.
            errors.addValidationError(UNROUTABLE_MESSAGE_PREFIX + resource + "].");
            return;
        }
        if (S3EndpointCheck.isPermittedHost(bucketHost, S3EndpointCheck.S3_SERVICE) == false) {
            errors.addValidationError(STEERED_MESSAGE_PREFIX + bucketHost + STEERED_MESSAGE_SUFFIX + resource + "].");
        }
    }

    /**
     * The host the SDK builds for this bucket name alone. Asking the resolver is what keeps this from
     * going stale: the bucket-name rules live in the SDK's endpoint ruleset and are positional rather
     * than suffix-shaped, so a name only reaches {@code s3-outposts} once it is long enough to carry an
     * outpost id, and a list of spellings maintained here would not know that.
     *
     * <p>Path-style addressing is forced so that an ordinary bucket appears in the path and leaves the
     * host bare, which is the spelling {@link S3EndpointCheck#isPermittedHost} accepts; a name that
     * steers the request keeps its own host either way. No endpoint is supplied, because a configured
     * endpoint does not suppress the steering this looks for.
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
