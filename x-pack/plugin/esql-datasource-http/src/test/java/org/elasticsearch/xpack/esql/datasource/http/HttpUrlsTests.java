/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.http;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class HttpUrlsTests extends ESTestCase {

    /** A pre-signed URL with credentials in its user info and a signature in its query string. */
    static final String SECRET_URL = "https://user:pass@host:8443/a/b.csv?X-Amz-Signature=abc";

    /** Asserts that {@code message} names the object {@link #SECRET_URL} points at without echoing either secret. */
    static void assertRedacted(String message) {
        assertThat(message, containsString("https://host:8443/a/b.csv"));
        assertThat(message, not(containsString("user:pass")));
        assertThat(message, not(containsString("X-Amz-Signature")));
    }

    public void testRedactDropsUserInfoQueryAndFragment() {
        assertEquals("https://host:8443/a/b.csv", HttpUrls.redact(StoragePath.of(SECRET_URL)));
        assertEquals("http://host/a.csv", HttpUrls.redact(StoragePath.of("http://host/a.csv#part")));
    }

    public void testRedactLeavesAPlainUrlUnchanged() {
        String plain = "https://example.com/file.parquet";
        assertEquals(plain, HttpUrls.redact(StoragePath.of(plain)));
    }
}
