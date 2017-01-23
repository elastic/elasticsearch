/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.job.persistence.UsagePersister;
import org.elasticsearch.xpack.ml.job.usage.UsageReporter;
import org.junit.Assert;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class CountingInputStreamTests extends ESTestCase {

    public void testRead_OneByteAtATime() throws IOException {

        UsageReporter usageReporter = new UsageReporter(Settings.EMPTY, "foo", Mockito.mock(UsagePersister.class));
        DummyDataCountsReporter dataCountsReporter = new DummyDataCountsReporter(usageReporter);

        final String TEXT = "123";
        InputStream source = new ByteArrayInputStream(TEXT.getBytes(StandardCharsets.UTF_8));

        try (CountingInputStream counting = new CountingInputStream(source, dataCountsReporter)) {
            while (counting.read() >= 0) {}
            Assert.assertEquals(TEXT.length(), usageReporter.getBytesReadSinceLastReport());

            Assert.assertEquals(usageReporter.getBytesReadSinceLastReport(), dataCountsReporter.getBytesRead());
        }
    }

    public void testRead_WithBuffer() throws IOException {
        final String TEXT = "To the man who only has a hammer, everything he encounters begins to look like a nail.";

        UsageReporter usageReporter = new UsageReporter(Settings.EMPTY, "foo", Mockito.mock(UsagePersister.class));
        DummyDataCountsReporter dataCountsReporter = new DummyDataCountsReporter(usageReporter);

        InputStream source = new ByteArrayInputStream(TEXT.getBytes(StandardCharsets.UTF_8));

        try (CountingInputStream counting = new CountingInputStream(source, dataCountsReporter)) {
            byte buf[] = new byte[256];
            while (counting.read(buf) >= 0) {}
            Assert.assertEquals(TEXT.length(), usageReporter.getBytesReadSinceLastReport());
            Assert.assertEquals(usageReporter.getBytesReadSinceLastReport(), dataCountsReporter.getBytesRead());
        }
    }

    public void testRead_WithTinyBuffer() throws IOException {
        final String TEXT = "To the man who only has a hammer, everything he encounters begins to look like a nail.";

        UsageReporter usageReporter = new UsageReporter(Settings.EMPTY, "foo", Mockito.mock(UsagePersister.class));
        DummyDataCountsReporter dataCountsReporter = new DummyDataCountsReporter(usageReporter);

        InputStream source = new ByteArrayInputStream(TEXT.getBytes(StandardCharsets.UTF_8));

        try (CountingInputStream counting = new CountingInputStream(source, dataCountsReporter)) {
            byte buf[] = new byte[8];
            while (counting.read(buf, 0, 8) >= 0) {}
            Assert.assertEquals(TEXT.length(), usageReporter.getBytesReadSinceLastReport());
            Assert.assertEquals(usageReporter.getBytesReadSinceLastReport(), dataCountsReporter.getBytesRead());
        }
    }

}
