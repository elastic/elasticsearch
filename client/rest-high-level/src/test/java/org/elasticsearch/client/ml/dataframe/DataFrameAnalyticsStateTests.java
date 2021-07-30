/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.dataframe;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

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

    public void testValue() {
        assertThat(DataFrameAnalyticsState.STARTING.value(), equalTo("starting"));
        assertThat(DataFrameAnalyticsState.STARTED.value(), equalTo("started"));
        assertThat(DataFrameAnalyticsState.REINDEXING.value(), equalTo("reindexing"));
        assertThat(DataFrameAnalyticsState.ANALYZING.value(), equalTo("analyzing"));
        assertThat(DataFrameAnalyticsState.STOPPING.value(), equalTo("stopping"));
        assertThat(DataFrameAnalyticsState.STOPPED.value(), equalTo("stopped"));
        assertThat(DataFrameAnalyticsState.FAILED.value(), equalTo("failed"));
    }
}
