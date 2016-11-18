/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.data;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.prelert.job.DataCounts;
import org.elasticsearch.xpack.prelert.job.process.autodetect.params.DataLoadParams;
import org.elasticsearch.xpack.prelert.utils.ExceptionsHelper;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DataStreamerThreadTests extends ESTestCase {
    private static final String JOB_ID = "foo";
    private static final String CONTENT_ENCODING = "application/json";

    private DataStreamer dataStreamer;
    private DataLoadParams params;
    private InputStream inputStream;

    private DataStreamerThread dataStreamerThread;

    @Before
    public void setUpMocks() {
        dataStreamer = Mockito.mock(DataStreamer.class);
        params = Mockito.mock(DataLoadParams.class);
        inputStream = Mockito.mock(InputStream.class);
        dataStreamerThread = new DataStreamerThread(dataStreamer, JOB_ID, CONTENT_ENCODING, params, inputStream);
    }

    @After
    public void verifyInputStreamClosed() throws IOException {
        verify(inputStream).close();
    }

    public void testRun() throws Exception {
        DataCounts counts = new DataCounts("foo", 42L, 0L, 0L, 0L, 0L, 0L, 0L, new Date(), new Date());
        when(dataStreamer.streamData(CONTENT_ENCODING, JOB_ID, inputStream, params)).thenReturn(counts);

        dataStreamerThread.run();

        assertEquals(JOB_ID, dataStreamerThread.getJobId());
        assertEquals(counts, dataStreamerThread.getDataCounts());
        assertFalse(dataStreamerThread.getIOException().isPresent());
        assertFalse(dataStreamerThread.getJobException().isPresent());
    }

    public void testRun_GivenIOException() throws Exception {
        when(dataStreamer.streamData(CONTENT_ENCODING, JOB_ID, inputStream, params)).thenThrow(new IOException("prelert"));

        dataStreamerThread.run();

        assertEquals(JOB_ID, dataStreamerThread.getJobId());
        assertNull(dataStreamerThread.getDataCounts());
        assertEquals("prelert", dataStreamerThread.getIOException().get().getMessage());
        assertFalse(dataStreamerThread.getJobException().isPresent());
    }

    public void testRun_GivenJobException() throws Exception {
        when(dataStreamer.streamData(CONTENT_ENCODING, JOB_ID, inputStream, params)).thenThrow(ExceptionsHelper.serverError("job failed"));

        dataStreamerThread.run();

        assertEquals(JOB_ID, dataStreamerThread.getJobId());
        assertNull(dataStreamerThread.getDataCounts());
        assertFalse(dataStreamerThread.getIOException().isPresent());
        assertEquals("job failed", dataStreamerThread.getJobException().get().getMessage());
    }
}
