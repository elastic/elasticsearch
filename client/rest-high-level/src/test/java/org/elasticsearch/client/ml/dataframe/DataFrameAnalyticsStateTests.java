/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
