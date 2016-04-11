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

package org.elasticsearch.action.ingest;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.ingest.core.IngestDocument;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.ingest.core.IngestDocumentTests.assertIngestDocument;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class SimulateProcessorResultTests extends ESTestCase {

    public void testSerialization() throws IOException {
        String processorTag = randomAsciiOfLengthBetween(1, 10);
        boolean isFailure = randomBoolean();
        SimulateProcessorResult simulateProcessorResult;
        if (isFailure) {
            simulateProcessorResult = new SimulateProcessorResult(processorTag, new IllegalArgumentException("test"));
        } else {
            IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
            simulateProcessorResult = new SimulateProcessorResult(processorTag, ingestDocument);
        }

        BytesStreamOutput out = new BytesStreamOutput();
        simulateProcessorResult.writeTo(out);
        StreamInput streamInput = StreamInput.wrap(out.bytes());
        SimulateProcessorResult otherSimulateProcessorResult = new SimulateProcessorResult(streamInput);
        assertThat(otherSimulateProcessorResult.getProcessorTag(), equalTo(simulateProcessorResult.getProcessorTag()));
        if (isFailure) {
            assertThat(simulateProcessorResult.getIngestDocument(), is(nullValue()));
            assertThat(otherSimulateProcessorResult.getFailure(), instanceOf(IllegalArgumentException.class));
            IllegalArgumentException e = (IllegalArgumentException) otherSimulateProcessorResult.getFailure();
            assertThat(e.getMessage(), equalTo("test"));
        } else {
            assertIngestDocument(otherSimulateProcessorResult.getIngestDocument(), simulateProcessorResult.getIngestDocument());
        }
    }
}
