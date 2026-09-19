/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import org.elasticsearch.common.ValidationException;

import java.util.List;
import java.util.Locale;

/**
 * Provider-specific resource validation for S3 URIs, invoked at {@code PUT /_query/dataset} time
 * via {@link org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceValidator#withResourceCheck}.
 *
 * <p>Refuses four forms that pass the scheme check and are not usable: an empty location, a
 * multi-region access point, an ARN, and an S3 Express directory bucket. The last is here rather than
 * with the endpoint rule because the bucket name alone moves the request to an {@code s3express} host,
 * so no endpoint setting can confine it.
 *
 * <p>Parsing is on the raw string: {@code StoragePath.of} throws {@code Malformed authority in location}
 * on an ARN before any check could run.
 */
class S3ResourceCheck {

    static final String INCOMPLETE_LOCATION_PREFIX = "[resource] is not a complete object location but was [";
    static final String MRAP_MESSAGE_PREFIX = "[resource] looks like a multi-region access point, which is not supported, but was [";
    static final String ARN_MESSAGE_PREFIX = "[resource] does not accept an ARN but was [";
    static final String ARN_MESSAGE_SUFFIX = "]. Use a bucket name, or an access point alias if the bucket is behind an access point.";
    static final String EXPRESS_MESSAGE_PREFIX = "[resource] looks like an S3 Express directory bucket, which is not supported, but was [";

    /**
     * The exact spellings the SDK keys off, both of which build an S3 Express endpoint. It matches the
     * last six characters against {@code --x-s3} and the last seven against {@code --xa-s3}, in separate
     * rules, so one suffix does not cover the other. {@code --op-s3} is deliberately absent: as a bucket
     * name it resolves to the ordinary regional host. An Outposts host is reached through an
     * {@code arn:aws:s3-outposts:} resource instead, which the ARN branch below refuses.
     */
    private static final List<String> DIRECTORY_BUCKET_SUFFIXES = List.of("--x-s3", "--xa-s3");

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
        }
    }
}
