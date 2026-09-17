/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import org.elasticsearch.common.ValidationException;

import java.util.Locale;

/**
 * Provider-specific resource validation for S3 URIs, invoked at {@code PUT /_query/dataset} time
 * via {@link org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceValidator#withResourceCheck}.
 *
 * <p>Four forms of resource URI are not usable by this product but pass the scheme check:
 * <ul>
 *   <li><b>Empty location</b> — {@code s3://} with no bucket (and therefore no key). The scheme
 *       matches, but the URI is not an object location.</li>
 *   <li><b>Multi-region access points (MRAP)</b> — not supported; the user must use a regional
 *       endpoint instead.</li>
 *   <li><b>ARN resources</b> — S3 does not accept ARNs as bucket identifiers in standard SDK calls;
 *       the user must use a bucket name or an access-point alias.</li>
 *   <li><b>S3 Express directory buckets</b> — not supported; the {@code --x-s3} bucket-name suffix alone
 *       moves the request to an {@code s3express-<az>} host, which no endpoint setting can constrain.</li>
 * </ul>
 *
 * <p>Parsing is done on the raw resource string. {@code StoragePath.of} must not be called here
 * because it throws {@code Malformed authority in location} on ARNs before any check can run. The SDK's
 * {@code Arn.fromString} is also not used — the string checks below cover all cases.
 */
class S3ResourceCheck {

    static final String INCOMPLETE_LOCATION_PREFIX = "[resource] is not a complete object location but was [";
    static final String MRAP_MESSAGE_PREFIX = "[resource] looks like a multi-region access point, which is not supported, but was [";
    static final String ARN_MESSAGE_PREFIX = "[resource] does not accept an ARN but was [";
    static final String ARN_MESSAGE_SUFFIX = "]. Use a bucket name, or an access point alias if the bucket is behind an access point.";
    static final String EXPRESS_MESSAGE_PREFIX = "[resource] looks like an S3 Express directory bucket, which is not supported, but was [";

    /**
     * Suffix that makes a bucket name a directory bucket. The SDK keys off this exact spelling: a bucket
     * named {@code mybucket--use1-az4--x-s3} resolves to an {@code s3express-<az>} host rather than the
     * regional S3 host, while {@code mybucket--use1-az4--x-s3-suffix} resolves to the ordinary one.
     */
    private static final String DIRECTORY_BUCKET_SUFFIX = "--x-s3";

    private S3ResourceCheck() {}

    /**
     * Validates that {@code resource} names an ordinary bucket (not an empty location, ARN, MRAP, or
     * directory bucket).
     * Adds a {@link ValidationException} error for each problem found; does not throw.
     *
     * <p>Empty authority is rejected first: {@code s3://} matches the scheme check but is not an
     * object location, and format inference would otherwise report that as "cannot determine a
     * format". The MRAP check runs next: an MRAP ARN would otherwise fall through to the generic
     * ARN branch and receive a misleading "use an access point alias" suggestion (which does not
     * exist for MRAPs).
     */
    static void validate(String resource, ValidationException errors) {
        // Extract the authority: everything between "://" and the first "/", lowercased for matching.
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

        // First path segment (the part of the path up to the next "/"), used for MRAP ARN detection.
        String firstPathSegmentLower = "";
        if (firstSlash >= 0) {
            String afterAuthority = afterScheme.substring(firstSlash + 1);
            int nextSlash = afterAuthority.indexOf('/');
            firstPathSegmentLower = (nextSlash < 0 ? afterAuthority : afterAuthority.substring(0, nextSlash)).toLowerCase(Locale.ROOT);
        }

        // 1. MRAP check: host ends with ".mrap" (short alias) or the AWS global FQDN suffix
        // ".mrap.accesspoint.s3-global.amazonaws.com" (what the AWS console / SDK resolves to),
        // OR it's an ARN whose first path segment ends with ".mrap".
        if (authorityLower.endsWith(".mrap")
            || authorityLower.endsWith(".mrap.accesspoint.s3-global.amazonaws.com")
            || (authorityLower.startsWith("arn:") && firstPathSegmentLower.endsWith(".mrap"))) {
            errors.addValidationError(MRAP_MESSAGE_PREFIX + resource + "].");
            return;
        }

        // 2. S3 Express directory buckets: the name alone moves the destination, so no endpoint can confine it.
        if (authorityLower.endsWith(DIRECTORY_BUCKET_SUFFIX)) {
            errors.addValidationError(EXPRESS_MESSAGE_PREFIX + resource + "].");
            return;
        }

        // 3. Generic ARN check.
        if (authorityLower.startsWith("arn:")) {
            errors.addValidationError(ARN_MESSAGE_PREFIX + resource + ARN_MESSAGE_SUFFIX);
        }
    }
}
