/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;

/**
 * What the tests that watch the checksums on S3 uploads have in common, which is deciding whether a request declares a checksum. Every
 * method decides what counts as a checksum header with the same rule, so a request is either checksum-free or not. The "no checksum" and
 * "some checksum" assertions can never both fail for the same headers.
 */
final class S3ChecksumRestTestSupport {

    private S3ChecksumRestTestSupport() {}

    private static boolean isChecksumRequestHeader(String headerName) {
        final var lowerCaseName = headerName.toLowerCase(Locale.ROOT);
        return lowerCaseName.startsWith("x-amz-checksum-")
            || lowerCaseName.equals("x-amz-trailer")
            || lowerCaseName.equals("x-amz-sdk-checksum-algorithm");
    }

    /** The request headers that declare a checksum or a checksum trailer, in a stable order so a failure message is readable. */
    static List<String> checksumRequestHeaderNames(Set<String> requestHeaderNames) {
        return requestHeaderNames.stream().filter(S3ChecksumRestTestSupport::isChecksumRequestHeader).sorted().toList();
    }

    /** Fails if any of the request headers declares a checksum or a checksum trailer. */
    static void assertNoChecksumRequestHeaders(Set<String> requestHeaderNames) {
        assertThat("unexpected checksum headers", checksumRequestHeaderNames(requestHeaderNames), empty());
    }

    /** Fails unless at least one of the request headers declares a checksum or a checksum trailer. */
    static void assertSomeChecksumRequestHeader(Set<String> requestHeaderNames) {
        assertThat(
            "expected a checksum header or trailer declaration in " + requestHeaderNames,
            checksumRequestHeaderNames(requestHeaderNames),
            not(empty())
        );
    }
}
