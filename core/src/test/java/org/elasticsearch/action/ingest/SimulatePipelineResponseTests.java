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
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.ingest.IngestDocumentTests.assertIngestDocument;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;

public class SimulatePipelineResponseTests extends ESTestCase {

    public void testSerialization() throws IOException {
        boolean isVerbose = randomBoolean();
        String id = randomBoolean() ? randomAsciiOfLengthBetween(1, 10) : null;
        int numResults = randomIntBetween(1, 10);
        List<SimulateDocumentResult> results = new ArrayList<>(numResults);
        for (int i = 0; i < numResults; i++) {
            boolean isFailure = randomBoolean();
            IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
            if (isVerbose) {
                int numProcessors = randomIntBetween(1, 10);
                List<SimulateProcessorResult> processorResults = new ArrayList<>(numProcessors);
                for (int j = 0; j < numProcessors; j++) {
                    String processorTag = randomAsciiOfLengthBetween(1, 10);
                    SimulateProcessorResult processorResult;
                    if (isFailure) {
                        processorResult = new SimulateProcessorResult(processorTag, new IllegalArgumentException("test"));
                    } else {
                        processorResult = new SimulateProcessorResult(processorTag, ingestDocument);
                    }
                    processorResults.add(processorResult);
                }
                results.add(new SimulateDocumentVerboseResult(processorResults));
            } else {
                results.add(new SimulateDocumentBaseResult(ingestDocument));
                SimulateDocumentBaseResult simulateDocumentBaseResult;
                if (isFailure) {
                    simulateDocumentBaseResult = new SimulateDocumentBaseResult(new IllegalArgumentException("test"));
                } else {
                    simulateDocumentBaseResult = new SimulateDocumentBaseResult(ingestDocument);
                }
                results.add(simulateDocumentBaseResult);
            }
        }

        SimulatePipelineResponse response = new SimulatePipelineResponse(id, isVerbose, results);
        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        SimulatePipelineResponse otherResponse = new SimulatePipelineResponse();
        otherResponse.readFrom(streamInput);

        assertThat(otherResponse.getPipelineId(), equalTo(response.getPipelineId()));
        assertThat(otherResponse.getResults().size(), equalTo(response.getResults().size()));

        Iterator<SimulateDocumentResult> expectedResultIterator = response.getResults().iterator();
        for (SimulateDocumentResult result : otherResponse.getResults()) {
            if (isVerbose) {
                SimulateDocumentVerboseResult expectedSimulateDocumentVerboseResult = (SimulateDocumentVerboseResult) expectedResultIterator.next();
                assertThat(result, instanceOf(SimulateDocumentVerboseResult.class));
                SimulateDocumentVerboseResult simulateDocumentVerboseResult = (SimulateDocumentVerboseResult) result;
                assertThat(simulateDocumentVerboseResult.getProcessorResults().size(), equalTo(expectedSimulateDocumentVerboseResult.getProcessorResults().size()));
                Iterator<SimulateProcessorResult> expectedProcessorResultIterator = expectedSimulateDocumentVerboseResult.getProcessorResults().iterator();
                for (SimulateProcessorResult simulateProcessorResult : simulateDocumentVerboseResult.getProcessorResults()) {
                    SimulateProcessorResult expectedProcessorResult = expectedProcessorResultIterator.next();
                    assertThat(simulateProcessorResult.getProcessorTag(), equalTo(expectedProcessorResult.getProcessorTag()));
                    if (simulateProcessorResult.getIngestDocument() != null) {
                        assertIngestDocument(simulateProcessorResult.getIngestDocument(), expectedProcessorResult.getIngestDocument());
                    }
                    if (expectedProcessorResult.getFailure() == null) {
                        assertThat(simulateProcessorResult.getFailure(), nullValue());
                    } else {
                        assertThat(simulateProcessorResult.getFailure(), instanceOf(IllegalArgumentException.class));
                        IllegalArgumentException e = (IllegalArgumentException) simulateProcessorResult.getFailure();
                        assertThat(e.getMessage(), equalTo("test"));
                    }
                }
            } else {
                SimulateDocumentBaseResult expectedSimulateDocumentBaseResult = (SimulateDocumentBaseResult) expectedResultIterator.next();
                assertThat(result, instanceOf(SimulateDocumentBaseResult.class));
                SimulateDocumentBaseResult simulateDocumentBaseResult = (SimulateDocumentBaseResult) result;
                if (simulateDocumentBaseResult.getIngestDocument() != null) {
                    assertIngestDocument(simulateDocumentBaseResult.getIngestDocument(), expectedSimulateDocumentBaseResult.getIngestDocument());
                }
                if (expectedSimulateDocumentBaseResult.getFailure() == null) {
                    assertThat(simulateDocumentBaseResult.getFailure(), nullValue());
                } else {
                    assertThat(simulateDocumentBaseResult.getFailure(), instanceOf(IllegalArgumentException.class));
                    IllegalArgumentException e = (IllegalArgumentException) simulateDocumentBaseResult.getFailure();
                    assertThat(e.getMessage(), equalTo("test"));
                }
            }
        }
    }
}
