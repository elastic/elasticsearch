/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
import org.elasticsearch.xpack.prelert.job.usage.UsageReporter;

public class CountingInputStreamTests extends ESTestCase {

    public void testRead_OneByteAtATime() throws IOException {

        UsageReporter usageReporter = new UsageReporter(Settings.EMPTY, "foo", Mockito.mock(UsagePersister.class));
        DummyStatusReporter statusReporter = new DummyStatusReporter(usageReporter);

        final String TEXT = "123";
        InputStream source = new ByteArrayInputStream(TEXT.getBytes(StandardCharsets.UTF_8));

        try (CountingInputStream counting = new CountingInputStream(source, statusReporter)) {
            while (counting.read() >= 0) {}
            Assert.assertEquals(TEXT.length(), usageReporter.getBytesReadSinceLastReport());

            Assert.assertEquals(usageReporter.getBytesReadSinceLastReport(), statusReporter.getBytesRead());
        }
    }

    public void testRead_WithBuffer() throws IOException {
        final String TEXT = "To the man who only has a hammer, everything he encounters begins to look like a nail.";

        UsageReporter usageReporter = new UsageReporter(Settings.EMPTY, "foo", Mockito.mock(UsagePersister.class));
        DummyStatusReporter statusReporter = new DummyStatusReporter(usageReporter);

        InputStream source = new ByteArrayInputStream(TEXT.getBytes(StandardCharsets.UTF_8));

        try (CountingInputStream counting = new CountingInputStream(source, statusReporter)) {
            byte buf[] = new byte[256];
            while (counting.read(buf) >= 0) {}
            Assert.assertEquals(TEXT.length(), usageReporter.getBytesReadSinceLastReport());
            Assert.assertEquals(usageReporter.getBytesReadSinceLastReport(), statusReporter.getBytesRead());
        }
    }

    public void testRead_WithTinyBuffer() throws IOException {
        final String TEXT = "To the man who only has a hammer, everything he encounters begins to look like a nail.";

        UsageReporter usageReporter = new UsageReporter(Settings.EMPTY, "foo", Mockito.mock(UsagePersister.class));
        DummyStatusReporter statusReporter = new DummyStatusReporter(usageReporter);

        InputStream source = new ByteArrayInputStream(TEXT.getBytes(StandardCharsets.UTF_8));

        try (CountingInputStream counting = new CountingInputStream(source, statusReporter)) {
            byte buf[] = new byte[8];
            while (counting.read(buf, 0, 8) >= 0) {}
            Assert.assertEquals(TEXT.length(), usageReporter.getBytesReadSinceLastReport());
            Assert.assertEquals(usageReporter.getBytesReadSinceLastReport(), statusReporter.getBytesRead());
        }
    }

}
