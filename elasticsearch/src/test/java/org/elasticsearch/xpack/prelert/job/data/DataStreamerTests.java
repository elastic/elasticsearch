/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.data;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.prelert.job.DataCounts;
import org.elasticsearch.xpack.prelert.job.process.autodetect.params.DataLoadParams;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DataStreamerTests extends ESTestCase {

    public void testConstructor_GivenNullDataProcessor() {

        ESTestCase.expectThrows(NullPointerException.class, () -> new DataStreamer(null));
    }

    public void testStreamData_GivenNoContentEncodingAndNoPersistBaseDir() throws IOException {

        DataProcessor dataProcessor = mock(DataProcessor.class);
        DataStreamer dataStreamer = new DataStreamer(dataProcessor);
        InputStream inputStream = mock(InputStream.class);
        DataLoadParams params = mock(DataLoadParams.class);

        when(dataProcessor.processData("foo", inputStream, params)).thenReturn(new DataCounts("foo"));

        dataStreamer.streamData("", "foo", inputStream, params);

        verify(dataProcessor).processData("foo", inputStream, params);
        Mockito.verifyNoMoreInteractions(dataProcessor);
    }

    public void testStreamData_ExpectsGzipButNotCompressed() throws IOException {
        DataProcessor dataProcessor = mock(DataProcessor.class);
        DataStreamer dataStreamer = new DataStreamer(dataProcessor);
        InputStream inputStream = mock(InputStream.class);
        DataLoadParams params = mock(DataLoadParams.class);

        try {
            dataStreamer.streamData("gzip", "foo", inputStream, params);
            fail("content encoding : gzip with uncompressed data should throw");
        } catch (IllegalArgumentException e) {
            assertEquals("Content-Encoding = gzip but the data is not in gzip format", e.getMessage());
        }
    }

    public void testStreamData_ExpectsGzipUsesGZipStream() throws IOException {
        PipedInputStream pipedIn = new PipedInputStream();
        PipedOutputStream pipedOut = new PipedOutputStream(pipedIn);
        try (GZIPOutputStream gzip = new GZIPOutputStream(pipedOut)) {
            gzip.write("Hello World compressed".getBytes(StandardCharsets.UTF_8));

            DataProcessor dataProcessor = mock(DataProcessor.class);
            DataStreamer dataStreamer = new DataStreamer(dataProcessor);
            DataLoadParams params = mock(DataLoadParams.class);

            when(dataProcessor.processData(Mockito.anyString(),
                    Mockito.any(InputStream.class),
                    Mockito.any(DataLoadParams.class)))
            .thenReturn(new DataCounts("foo"));

            dataStreamer.streamData("gzip", "foo", pipedIn, params);

            // submitDataLoadJob should be called with a GZIPInputStream
            ArgumentCaptor<InputStream> streamArg = ArgumentCaptor.forClass(InputStream.class);

            verify(dataProcessor).processData(Mockito.anyString(),
                    streamArg.capture(),
                    Mockito.any(DataLoadParams.class));

            assertTrue(streamArg.getValue() instanceof GZIPInputStream);
        }
    }
}
