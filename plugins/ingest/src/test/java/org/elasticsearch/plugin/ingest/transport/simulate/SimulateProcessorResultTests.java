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
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class SimulateProcessorResultTests extends ESTestCase {

    public void testSerialization() throws IOException {
        String processorId = randomAsciiOfLengthBetween(1, 10);
        boolean isFailure = randomBoolean();
        SimulateProcessorResult simulateProcessorResult;
        if (isFailure) {
            simulateProcessorResult = new SimulateProcessorResult(processorId, new IllegalArgumentException("test"));
        } else {
            IngestDocument ingestDocument = new IngestDocument(randomAsciiOfLengthBetween(1, 10), randomAsciiOfLengthBetween(1, 10), randomAsciiOfLengthBetween(1, 10),
                    Collections.singletonMap(randomAsciiOfLengthBetween(1, 10), randomAsciiOfLengthBetween(1, 10)));
            simulateProcessorResult = new SimulateProcessorResult(processorId, ingestDocument);
        }

        BytesStreamOutput out = new BytesStreamOutput();
        simulateProcessorResult.writeTo(out);
        StreamInput streamInput = StreamInput.wrap(out.bytes());
        SimulateProcessorResult otherSimulateProcessorResult = SimulateProcessorResult.readSimulateProcessorResultFrom(streamInput);
        assertThat(otherSimulateProcessorResult.getProcessorId(), equalTo(simulateProcessorResult.getProcessorId()));
        assertThat(otherSimulateProcessorResult.getData(), equalTo(simulateProcessorResult.getData()));
        if (isFailure) {
            assertThat(otherSimulateProcessorResult.getFailure(), instanceOf(IllegalArgumentException.class));
            IllegalArgumentException e = (IllegalArgumentException) otherSimulateProcessorResult.getFailure();
            assertThat(e.getMessage(), equalTo("test"));
        }
    }
}
