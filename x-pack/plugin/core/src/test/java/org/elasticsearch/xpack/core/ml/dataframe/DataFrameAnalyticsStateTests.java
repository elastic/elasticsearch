/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class DataFrameAnalyticsStateTests extends ESTestCase {

    public void testFromString() {
        assertThat(DataFrameAnalyticsState.fromString("started"), equalTo(DataFrameAnalyticsState.STARTED));
        assertThat(DataFrameAnalyticsState.fromString("reindexing"), equalTo(DataFrameAnalyticsState.REINDEXING));
        assertThat(DataFrameAnalyticsState.fromString("analyzing"), equalTo(DataFrameAnalyticsState.ANALYZING));
        assertThat(DataFrameAnalyticsState.fromString("stopping"), equalTo(DataFrameAnalyticsState.STOPPING));
        assertThat(DataFrameAnalyticsState.fromString("stopped"), equalTo(DataFrameAnalyticsState.STOPPED));
        assertThat(DataFrameAnalyticsState.fromString("failed"), equalTo(DataFrameAnalyticsState.FAILED));
    }

    public void testToString() {
        assertThat(DataFrameAnalyticsState.STARTED.toString(), equalTo("started"));
        assertThat(DataFrameAnalyticsState.REINDEXING.toString(), equalTo("reindexing"));
        assertThat(DataFrameAnalyticsState.ANALYZING.toString(), equalTo("analyzing"));
        assertThat(DataFrameAnalyticsState.STOPPING.toString(), equalTo("stopping"));
        assertThat(DataFrameAnalyticsState.STOPPED.toString(), equalTo("stopped"));
        assertThat(DataFrameAnalyticsState.FAILED.toString(), equalTo("failed"));
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
