/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.apmintegration;

import org.elasticsearch.test.ESTestCase;

import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * Tests for {@link RecordingApmServer}.
 */
public class RecordingApmServerTests extends ESTestCase {

    /**
     * Regression test for <a href="https://github.com/elastic/elasticsearch/issues/149945">#149945</a>.
     * <p>
     * Sends a chunked POST where the connection closes after the terminating {@code 0\r\n} but
     * before the required trailing {@code \r\n}.  This causes {@code consumeCRLF()} inside
     * {@code ChunkedInputStream} to throw after {@code eof} has already been set to {@code true}.
     * Without the fix, the resulting {@link AssertionError} in the JDK HTTP-server dispatcher
     * thread is captured by {@link org.apache.lucene.tests.util.LuceneTestCase} and fails this test.
     */
    public void testTruncatedChunkedBodyDoesNotCrashDispatcher() throws Exception {
        RecordingApmServer server = new RecordingApmServer();
        try {
            server.before();
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
        int port = server.getPort();

        try (Socket socket = new Socket(InetAddress.getByName("127.0.0.1"), port)) {
            OutputStream out = socket.getOutputStream();
            out.write(
                ("POST /intake/v2/events HTTP/1.1\r\n"
                    + "Host: 127.0.0.1:"
                    + port
                    + "\r\n"
                    + "Transfer-Encoding: chunked\r\n"
                    + "Content-Type: application/x-ndjson\r\n"
                    + "\r\n"
                    + "5\r\nhello\r\n"
                    + "0\r\n"  // terminating chunk header — missing the required trailing \r\n
                ).getBytes(StandardCharsets.US_ASCII)
            );
            out.flush();
            socket.shutdownOutput(); // FIN: causes consumeCRLF() to see EOF and throw
        }

        server.after();
    }
}
