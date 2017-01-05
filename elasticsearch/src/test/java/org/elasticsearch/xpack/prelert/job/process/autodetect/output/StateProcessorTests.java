/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.process.autodetect.output;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.prelert.job.persistence.JobResultsPersister;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests for reading state from the native process.
 */
public class StateProcessorTests extends ESTestCase {

    private static final String STATE_SAMPLE = "first header\n"
            + "first data\n"
            + "\0"
            + "second header\n"
            + "second data\n"
            + "\0"
            + "third header\n"
            + "third data\n"
            + "\0";

    private static final int NUM_LARGE_DOCS = 2;
    private static final int LARGE_DOC_SIZE = 16000000;

    public void testStateRead() throws IOException {
        ByteArrayInputStream stream = new ByteArrayInputStream(STATE_SAMPLE.getBytes(StandardCharsets.UTF_8));

        ArgumentCaptor<BytesReference> bytesRefCaptor = ArgumentCaptor.forClass(BytesReference.class);
        JobResultsPersister persister = Mockito.mock(JobResultsPersister.class);

        StateProcessor stateParser = new StateProcessor(Settings.EMPTY, persister);
        stateParser.process("_id", stream);

        verify(persister, times(3)).persistBulkState(eq("_id"), bytesRefCaptor.capture());

        String[] threeStates = STATE_SAMPLE.split("\0");
        List<BytesReference> capturedBytes = bytesRefCaptor.getAllValues();
        assertEquals(threeStates[0], capturedBytes.get(0).utf8ToString());
        assertEquals(threeStates[1], capturedBytes.get(1).utf8ToString());
        assertEquals(threeStates[2], capturedBytes.get(2).utf8ToString());
    }

    /**
     * This test is designed to pick up N-squared processing in the state consumption code.
     * The size of the state document is comparable to those that the C++ code will create for a huge model.
     */
    public void testLargeStateRead() throws Exception {
        StringBuilder builder = new StringBuilder(NUM_LARGE_DOCS * (LARGE_DOC_SIZE + 10)); // 10 for header and separators
        for (int docNum = 1; docNum <= NUM_LARGE_DOCS; ++docNum) {
            builder.append("header").append(docNum).append("\n");
            for (int count = 0; count < (LARGE_DOC_SIZE / "data".length()); ++count) {
                builder.append("data");
            }
            builder.append("\n\0");
        }

        ByteArrayInputStream stream = new ByteArrayInputStream(builder.toString().getBytes(StandardCharsets.UTF_8));

        JobResultsPersister persister = Mockito.mock(JobResultsPersister.class);

        StateProcessor stateParser = new StateProcessor(Settings.EMPTY, persister);

        // 5 seconds is an overestimate to avoid spurious failures due to VM stalls - on a
        // reasonable spec laptop this should take around 1 second
        assertBusy(() -> stateParser.process("_id", stream), 5, TimeUnit.SECONDS);

        verify(persister, times(NUM_LARGE_DOCS)).persistBulkState(eq("_id"), any());
    }
}
