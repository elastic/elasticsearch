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
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;

public class SimulatePipelineResponseTests extends AbstractXContentTestCase<SimulatePipelineResponse> {

    public void testSerialization() throws IOException {
        boolean isVerbose = randomBoolean();
        String id = randomBoolean() ? randomAlphaOfLengthBetween(1, 10) : null;

        SimulatePipelineResponse response = createInstance(id, isVerbose, true);
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

    public static SimulatePipelineResponse createInstance(String pipelineId, boolean isVerbose, boolean withFailure) {
        int numResults = randomIntBetween(1, 10);
        List<SimulateDocumentResult> results = new ArrayList<>(numResults);
        for (int i = 0; i < numResults; i++) {
            boolean isFailure = withFailure && randomBoolean();
            IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
            if (isVerbose) {
                int numProcessors = randomIntBetween(1, 10);
                List<SimulateProcessorResult> processorResults = new ArrayList<>(numProcessors);
                for (int j = 0; j < numProcessors; j++) {
                    String processorTag = randomAlphaOfLengthBetween(1, 10);
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
        return new SimulatePipelineResponse(pipelineId, isVerbose, results);
    }

    private static SimulatePipelineResponse createTestInstanceWithFailures() {
        boolean isVerbose = randomBoolean();
        return createInstance(null, isVerbose, false);
    }

    @Override
    protected SimulatePipelineResponse createTestInstance() {
        boolean isVerbose = randomBoolean();
        // since the pipeline id is not serialized with XContent we set it to null for equality tests.
        // we test failures separately since comparing XContent is not possible with failures
        return createInstance(null, isVerbose, false);
    }

    @Override
    protected SimulatePipelineResponse doParseInstance(XContentParser parser) {
        return SimulatePipelineResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected void assertEqualInstances(SimulatePipelineResponse response,
                                        SimulatePipelineResponse parsedResponse) {
        assertEquals(response.getPipelineId(), parsedResponse.getPipelineId());
        assertEquals(response.isVerbose(), parsedResponse.isVerbose());
        assertEquals(response.getResults().size(), parsedResponse.getResults().size());
        for (int i=0; i < response.getResults().size(); i++) {
            if (response.isVerbose()) {
                assert response.getResults().get(i) instanceof SimulateDocumentVerboseResult;
                assert parsedResponse.getResults().get(i) instanceof SimulateDocumentVerboseResult;
                SimulateDocumentVerboseResult responseResult = (SimulateDocumentVerboseResult)response.getResults().get(i);
                SimulateDocumentVerboseResult parsedResult = (SimulateDocumentVerboseResult)parsedResponse.getResults().get(i);
                SimulateDocumentVerboseResultTests.assertEqualDocs(responseResult, parsedResult);
            } else {
                assert response.getResults().get(i) instanceof SimulateDocumentBaseResult;
                assert parsedResponse.getResults().get(i) instanceof SimulateDocumentBaseResult;
                SimulateDocumentBaseResult responseResult = (SimulateDocumentBaseResult)response.getResults().get(i);
                SimulateDocumentBaseResult parsedResult = (SimulateDocumentBaseResult)parsedResponse.getResults().get(i);
                SimulateDocumentBaseResultTests.assertEqualDocs(responseResult, parsedResult);
            }
        }
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // We cannot have random fields in the _source field
        return field -> field.contains("doc._source") || field.contains("doc._ingest");
    }

    /**
     * Test parsing {@link SimulatePipelineResponse} with inner failures as they don't support asserting on xcontent equivalence, given that
     * exceptions are not parsed back as the same original class. We run the usual {@link AbstractXContentTestCase#testFromXContent()}
     * without failures, and this other test with failures where we disable asserting on xcontent equivalence at the end.
     */
    public void testFromXContentWithFailures() throws IOException {
        Supplier<SimulatePipelineResponse> instanceSupplier = SimulatePipelineResponseTests::createTestInstanceWithFailures;
        //exceptions are not of the same type whenever parsed back
        boolean assertToXContentEquivalence = false;
        AbstractXContentTestCase.testFromXContent(NUMBER_OF_TEST_RUNS, instanceSupplier, supportsUnknownFields(), getShuffleFieldsExceptions(),
            getRandomFieldsExcludeFilter(), this::createParser, this::doParseInstance,
            this::assertEqualInstances, assertToXContentEquivalence, getToXContentParams());
    }
}
