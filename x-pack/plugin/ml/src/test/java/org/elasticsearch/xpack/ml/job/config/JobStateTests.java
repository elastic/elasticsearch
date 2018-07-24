/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JobStateTests extends ESTestCase {

    public void testFromString() {
        assertEquals(JobState.fromString("closing"), JobState.CLOSING);
        assertEquals(JobState.fromString("closed"), JobState.CLOSED);
        assertEquals(JobState.fromString("failed"), JobState.FAILED);
        assertEquals(JobState.fromString("opened"), JobState.OPENED);
        assertEquals(JobState.fromString("CLOSING"), JobState.CLOSING);
        assertEquals(JobState.fromString("CLOSED"), JobState.CLOSED);
        assertEquals(JobState.fromString("FAILED"), JobState.FAILED);
        assertEquals(JobState.fromString("OPENED"), JobState.OPENED);
    }

    public void testToString() {
        assertEquals("closing", JobState.CLOSING.toString());
        assertEquals("closed", JobState.CLOSED.toString());
        assertEquals("failed", JobState.FAILED.toString());
        assertEquals("opened", JobState.OPENED.toString());
    }

    public void testValidOrdinals() {
        assertEquals(0, JobState.CLOSING.ordinal());
        assertEquals(1, JobState.CLOSED.ordinal());
        assertEquals(2, JobState.OPENED.ordinal());
        assertEquals(3, JobState.FAILED.ordinal());
        assertEquals(4, JobState.OPENING.ordinal());
    }

    public void testIsAnyOf() {
        assertFalse(JobState.OPENED.isAnyOf());
        assertFalse(JobState.OPENING.isAnyOf(JobState.CLOSED, JobState.FAILED));
        assertFalse(JobState.OPENED.isAnyOf(JobState.CLOSED, JobState.FAILED));
        assertFalse(JobState.CLOSED.isAnyOf(JobState.FAILED, JobState.OPENED));
        assertFalse(JobState.CLOSING.isAnyOf(JobState.FAILED, JobState.OPENED));

        assertTrue(JobState.OPENING.isAnyOf(JobState.OPENED, JobState.OPENING));
        assertTrue(JobState.OPENED.isAnyOf(JobState.OPENED, JobState.CLOSED));
        assertTrue(JobState.OPENED.isAnyOf(JobState.OPENED, JobState.CLOSED));
        assertTrue(JobState.CLOSED.isAnyOf(JobState.CLOSED));
        assertTrue(JobState.CLOSING.isAnyOf(JobState.CLOSING));
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

        // OPENING state was introduced in v5.5.
        // Pre v5.5 its translated as CLOSED
        JobState.OPENING.writeTo(out);
        assertEquals(JobState.CLOSED, enumCaptor.getValue());

        when(out.getVersion()).thenReturn(Version.V_5_5_0);

        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocationOnMock) {
                return null;
            }
        }).when(out).writeEnum(enumCaptor.capture());

        JobState.OPENING.writeTo(out);
        assertEquals(JobState.OPENING, enumCaptor.getValue());
    }
}
