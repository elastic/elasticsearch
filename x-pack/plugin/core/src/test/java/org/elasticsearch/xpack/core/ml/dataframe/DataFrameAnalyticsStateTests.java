/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DataFrameAnalyticsStateTests extends ESTestCase {

    public void testFromString() {
        assertThat(DataFrameAnalyticsState.fromString("starting"), equalTo(DataFrameAnalyticsState.STARTING));
        assertThat(DataFrameAnalyticsState.fromString("started"), equalTo(DataFrameAnalyticsState.STARTED));
        assertThat(DataFrameAnalyticsState.fromString("reindexing"), equalTo(DataFrameAnalyticsState.REINDEXING));
        assertThat(DataFrameAnalyticsState.fromString("analyzing"), equalTo(DataFrameAnalyticsState.ANALYZING));
        assertThat(DataFrameAnalyticsState.fromString("stopping"), equalTo(DataFrameAnalyticsState.STOPPING));
        assertThat(DataFrameAnalyticsState.fromString("stopped"), equalTo(DataFrameAnalyticsState.STOPPED));
        assertThat(DataFrameAnalyticsState.fromString("failed"), equalTo(DataFrameAnalyticsState.FAILED));
    }

    public void testToString() {
        assertThat(DataFrameAnalyticsState.STARTING.toString(), equalTo("starting"));
        assertThat(DataFrameAnalyticsState.STARTED.toString(), equalTo("started"));
        assertThat(DataFrameAnalyticsState.REINDEXING.toString(), equalTo("reindexing"));
        assertThat(DataFrameAnalyticsState.ANALYZING.toString(), equalTo("analyzing"));
        assertThat(DataFrameAnalyticsState.STOPPING.toString(), equalTo("stopping"));
        assertThat(DataFrameAnalyticsState.STOPPED.toString(), equalTo("stopped"));
        assertThat(DataFrameAnalyticsState.FAILED.toString(), equalTo("failed"));
    }

    public void testWriteStartingStateToPost75() throws IOException {
        StreamOutput streamOutput = mock(StreamOutput.class);
        when(streamOutput.getVersion()).thenReturn(Version.V_7_5_0);
        DataFrameAnalyticsState.STARTING.writeTo(streamOutput);
        verify(streamOutput, times(1)).writeEnum(DataFrameAnalyticsState.STARTING);
    }

    public void testIsAnyOf() {
        assertThat(DataFrameAnalyticsState.STARTED.isAnyOf(), is(false));
        assertThat(DataFrameAnalyticsState.STARTED.isAnyOf(DataFrameAnalyticsState.STARTED), is(true));
        assertThat(DataFrameAnalyticsState.STARTED.isAnyOf(DataFrameAnalyticsState.ANALYZING, DataFrameAnalyticsState.STOPPED), is(false));
        assertThat(DataFrameAnalyticsState.STARTED.isAnyOf(DataFrameAnalyticsState.STARTED, DataFrameAnalyticsState.STOPPED), is(true));
        assertThat(DataFrameAnalyticsState.ANALYZING.isAnyOf(DataFrameAnalyticsState.STARTED, DataFrameAnalyticsState.STOPPED), is(false));
        assertThat(DataFrameAnalyticsState.ANALYZING.isAnyOf(DataFrameAnalyticsState.ANALYZING, DataFrameAnalyticsState.FAILED), is(true));
    }
}
