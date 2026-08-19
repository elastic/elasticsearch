/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Guards the {@code sun.net.httpserver.maxReqTime} system property that {@code ElasticsearchTestBasePlugin} sets for
 * test JVMs.
 *
 * <p>Without it the property defaults to {@code -1}, so a {@code com.sun.net.httpserver} worker will block forever
 * reading a request whose header block never terminates, making the mock HTTP servers used by the blob store repository
 * tests unresponsive until the client's own read timeout fires. That is what failed
 * {@code GoogleCloudStorageBlobStoreRepositoryTests#testSnapshotWithLargeSegmentFiles} in
 * <a href="https://github.com/elastic/elasticsearch/issues/156468">156468</a>.
 *
 * <p>This deliberately asserts only that the property is configured, which is the realistic regression: someone
 * removing or renaming the build setting. Asserting the <em>behaviour</em> — that the server really does close such a
 * request — requires waiting out the timeout, which costs more wall clock than a guard of this value justifies. That
 * behaviour was verified manually when the setting was introduced, and {@link MockHttpServerStallWatchdog} reports any
 * occurrence at runtime.
 */
public class MockHttpServerRequestTimeoutTests extends ESTestCase {

    public void testRequestReadTimeoutIsConfiguredForTestJvms() {
        final String maxReqTime = System.getProperty("sun.net.httpserver.maxReqTime");
        assertThat(
            "sun.net.httpserver.maxReqTime should be set for test JVMs by ElasticsearchTestBasePlugin; "
                + "without it a mock HTTP server worker can block forever on an incomplete request",
            maxReqTime,
            notNullValue()
        );
        assertThat("sun.net.httpserver.maxReqTime should be a positive number of seconds", Integer.parseInt(maxReqTime), greaterThan(0));
    }
}
