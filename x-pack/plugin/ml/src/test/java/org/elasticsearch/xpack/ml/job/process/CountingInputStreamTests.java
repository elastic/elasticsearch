/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process;

import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class CountingInputStreamTests extends ESTestCase {

    public void testRead_OneByteAtATime() throws IOException {

        DummyDataCountsReporter dataCountsReporter = new DummyDataCountsReporter();

        final String TEXT = "123";
        InputStream source = new ByteArrayInputStream(TEXT.getBytes(StandardCharsets.UTF_8));

        try (CountingInputStream counting = new CountingInputStream(source, dataCountsReporter)) {
            while (counting.read() >= 0) {}
            assertEquals(TEXT.length(), dataCountsReporter.incrementalStats().getInputBytes());
        }
    }

    public void testRead_WithBuffer() throws IOException {
        final String TEXT = "To the man who only has a hammer, everything he encounters begins to look like a nail.";

        DummyDataCountsReporter dataCountsReporter = new DummyDataCountsReporter();

        InputStream source = new ByteArrayInputStream(TEXT.getBytes(StandardCharsets.UTF_8));

        try (CountingInputStream counting = new CountingInputStream(source, dataCountsReporter)) {
            byte buf[] = new byte[256];
            while (counting.read(buf) >= 0) {}
            assertEquals(TEXT.length(), dataCountsReporter.incrementalStats().getInputBytes());
        }
    }

    public void testRead_WithTinyBuffer() throws IOException {
        final String TEXT = "To the man who only has a hammer, everything he encounters begins to look like a nail.";

        DummyDataCountsReporter dataCountsReporter = new DummyDataCountsReporter();

        InputStream source = new ByteArrayInputStream(TEXT.getBytes(StandardCharsets.UTF_8));

        try (CountingInputStream counting = new CountingInputStream(source, dataCountsReporter)) {
            byte buf[] = new byte[8];
            while (counting.read(buf, 0, 8) >= 0) {}
            assertEquals(TEXT.length(), dataCountsReporter.incrementalStats().getInputBytes());
        }
    }

    public void testRead_WithResets() throws IOException {

        DummyDataCountsReporter dataCountsReporter = new DummyDataCountsReporter();

        final String TEXT = "To the man who only has a hammer, everything he encounters begins to look like a nail.";
        InputStream source = new ByteArrayInputStream(TEXT.getBytes(StandardCharsets.UTF_8));

        try (CountingInputStream counting = new CountingInputStream(source, dataCountsReporter)) {
            while (counting.read() >= 0) {
                if (randomInt(10) > 5) {
                    counting.mark(-1);
                }
                if (randomInt(10) > 7) {
                    counting.reset();
                }
            }
            assertEquals(TEXT.length(), dataCountsReporter.incrementalStats().getInputBytes());
        }
    }

}
