/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.process;

import com.carrotsearch.randomizedtesting.annotations.Timeout;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.mock.orig.Mockito;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterService;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for reading state from the native process.
 */
public class IndexingStateProcessorTests extends ESTestCase {

    private static final String STATE_SAMPLE = ""
            + "{\"index\": {\"_index\": \"test\", \"_id\": \"1\"}}\n"
            + "{ \"field\" : \"value1\" }\n"
            + "\0"
            + "{\"index\": {\"_index\": \"test\", \"_id\": \"2\"}}\n"
            + "{ \"field\" : \"value2\" }\n"
            + "\0"
            + "{\"index\": {\"_index\": \"test\", \"_id\": \"3\"}}\n"
            + "{ \"field\" : \"value3\" }\n"
            + "\0";

    private static final String JOB_ID = "state-processor-test-job";

    private static final int NUM_LARGE_DOCS = 2;
    private static final int LARGE_DOC_SIZE = 1000000;

    private IndexingStateProcessor stateProcessor;
    private ResultsPersisterService resultsPersisterService;

    @Before
    public void initialize() {
        resultsPersisterService = mock(ResultsPersisterService.class);
        AnomalyDetectionAuditor auditor = mock(AnomalyDetectionAuditor.class);
        stateProcessor = spy(new IndexingStateProcessor(JOB_ID, resultsPersisterService, auditor));
        when(resultsPersisterService.bulkIndexWithRetry(any(BulkRequest.class), any(), any(), any())).thenReturn(mock(BulkResponse.class));
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
    }

    @After
    public void verifyNoMoreClientInteractions() {
        Mockito.verifyNoMoreInteractions(resultsPersisterService);
    }

    public void testStateRead() throws IOException {
        ByteArrayInputStream stream = new ByteArrayInputStream(STATE_SAMPLE.getBytes(StandardCharsets.UTF_8));
        stateProcessor.process(stream);
        ArgumentCaptor<BytesReference> bytesRefCaptor = ArgumentCaptor.forClass(BytesReference.class);
        verify(stateProcessor, times(3)).persist(bytesRefCaptor.capture());

        String[] threeStates = STATE_SAMPLE.split("\0");
        List<BytesReference> capturedBytes = bytesRefCaptor.getAllValues();
        assertEquals(threeStates[0], capturedBytes.get(0).utf8ToString());
        assertEquals(threeStates[1], capturedBytes.get(1).utf8ToString());
        assertEquals(threeStates[2], capturedBytes.get(2).utf8ToString());
        verify(resultsPersisterService, times(3)).bulkIndexWithRetry(any(BulkRequest.class), any(), any(), any());
    }

    public void testStateReadGivenConsecutiveZeroBytes() throws IOException {
        String zeroBytes = "\0\0\0\0\0\0";
        ByteArrayInputStream stream = new ByteArrayInputStream(zeroBytes.getBytes(StandardCharsets.UTF_8));

        stateProcessor.process(stream);

        verify(stateProcessor, never()).persist(any());
        Mockito.verifyNoMoreInteractions(resultsPersisterService);
    }

    public void testStateReadGivenConsecutiveSpacesFollowedByZeroByte() throws IOException {
        String zeroBytes = "        \n\0";
        ByteArrayInputStream stream = new ByteArrayInputStream(zeroBytes.getBytes(StandardCharsets.UTF_8));

        stateProcessor.process(stream);

        verify(stateProcessor, times(1)).persist(any());
        Mockito.verifyNoMoreInteractions(resultsPersisterService);
    }

    /**
     * This test is designed to pick up N-squared processing in the state consumption code.  The size of the state document
     * is comparable to those that the C++ code will create for a huge model.  10 seconds is an overestimate of the time
     * required to avoid spurious failures due to VM stalls - on a reasonable spec laptop this should take around 1 second.
     */
    @Timeout(millis = 10 * 1000)
    public void testLargeStateRead() throws Exception {
        StringBuilder builder = new StringBuilder(NUM_LARGE_DOCS * (LARGE_DOC_SIZE + 10)); // 10 for header and separators
        for (int docNum = 1; docNum <= NUM_LARGE_DOCS; ++docNum) {
            builder.append("{\"index\":{\"_index\":\"header").append(docNum).append("\"}}\n");
            for (int count = 0; count < (LARGE_DOC_SIZE / "data".length()); ++count) {
                builder.append("data");
            }
            builder.append("\n\0");
        }

        ByteArrayInputStream stream = new ByteArrayInputStream(builder.toString().getBytes(StandardCharsets.UTF_8));
        stateProcessor.process(stream);
        verify(stateProcessor, times(NUM_LARGE_DOCS)).persist(any());
        verify(resultsPersisterService, times(NUM_LARGE_DOCS)).bulkIndexWithRetry(any(BulkRequest.class), any(), any(), any());
    }
}
