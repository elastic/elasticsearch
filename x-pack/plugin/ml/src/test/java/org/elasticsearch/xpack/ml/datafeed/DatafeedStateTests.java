/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DatafeedStateTests extends ESTestCase {

    public void testFromString() {
        assertEquals(DatafeedState.fromString("started"), DatafeedState.STARTED);
        assertEquals(DatafeedState.fromString("stopped"), DatafeedState.STOPPED);
    }

    public void testToString() {
        assertEquals("started", DatafeedState.STARTED.toString());
        assertEquals("stopped", DatafeedState.STOPPED.toString());
    }

    public void testValidOrdinals() {
        assertEquals(0, DatafeedState.STARTED.ordinal());
        assertEquals(1, DatafeedState.STOPPED.ordinal());
        assertEquals(2, DatafeedState.STARTING.ordinal());
        assertEquals(3, DatafeedState.STOPPING.ordinal());
    }

    @SuppressWarnings("unchecked")
    public void testStreaming_v54BackwardsCompatibility() throws IOException {
        StreamOutput out = mock(StreamOutput.class);
        when(out.getVersion()).thenReturn(Version.V_5_4_0);
        ArgumentCaptor<Enum> enumCaptor = ArgumentCaptor.forClass(Enum.class);

        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocationOnMock) {
                return null;
            }
        }).when(out).writeEnum(enumCaptor.capture());

        // STARTING & STOPPING states were introduced in v5.5.
        // Pre v5.5 STARTING translated as STOPPED
        DatafeedState.STARTING.writeTo(out);
        assertEquals(DatafeedState.STOPPED, enumCaptor.getValue());

        // Pre v5.5 STOPPING means the datafeed is STARTED
        DatafeedState.STOPPING.writeTo(out);
        assertEquals(DatafeedState.STARTED, enumCaptor.getValue());

        // POST 5.5 enums a written as is
        when(out.getVersion()).thenReturn(Version.V_5_5_0);

        DatafeedState.STARTING.writeTo(out);
        assertEquals(DatafeedState.STARTING, enumCaptor.getValue());
        DatafeedState.STOPPING.writeTo(out);
        assertEquals(DatafeedState.STOPPING, enumCaptor.getValue());
    }
}
