/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.plugin.ingest.transport.simulate;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.ingest.Data;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class SimulateProcessorResultTests extends ESTestCase {
    private Data data;
    private SimulateProcessorResult result;
    private SimulateProcessorResult failedResult;
    private String processorId;
    private Throwable throwable;

    @Before
    public void setup() {
        data = new Data("_index", "_type", "_id", Collections.singletonMap("foo", "bar"));
        processorId = "id";
        throwable = new IllegalArgumentException("foo");
        result = new SimulateProcessorResult(processorId, data);
        failedResult = new SimulateProcessorResult(processorId, throwable);
    }

    public void testEqualsData() {
        SimulateProcessorResult otherResult = new SimulateProcessorResult(new String(processorId), new Data(data));
        assertThat(result, equalTo(otherResult));
    }

    public void testEqualsSameClassThrowable() {
        SimulateProcessorResult otherFailedResult = new SimulateProcessorResult(new String(processorId), new IllegalArgumentException("foo"));
        assertThat(failedResult, equalTo(otherFailedResult));
    }

    public void testNotEqualsThrowable() {
        SimulateProcessorResult otherFailedResult = new SimulateProcessorResult(new String(processorId), new NullPointerException("foo"));
        assertThat(failedResult, not(equalTo(otherFailedResult)));
    }

    public void testStreamableWithThrowable() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        failedResult.writeTo(out);

        StreamInput streamInput = StreamInput.wrap(out.bytes());
        SimulateProcessorResult otherFailedResult = new SimulateProcessorResult();
        otherFailedResult.readFrom(streamInput);

        assertThat(failedResult, equalTo(otherFailedResult));
    }

    public void testStreamableWithData() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        result.writeTo(out);

        StreamInput streamInput = StreamInput.wrap(out.bytes());
        SimulateProcessorResult otherResult = new SimulateProcessorResult();
        otherResult.readFrom(streamInput);

        assertThat(result, equalTo(otherResult));

    }
}
