/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.ingest;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringJoiner;
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
        SimulatePipelineResponse otherResponse = new SimulatePipelineResponse(streamInput);

        assertThat(otherResponse.getPipelineId(), equalTo(response.getPipelineId()));
        assertThat(otherResponse.getResults().size(), equalTo(response.getResults().size()));

        Iterator<SimulateDocumentResult> expectedResultIterator = response.getResults().iterator();
        for (SimulateDocumentResult result : otherResponse.getResults()) {
            if (isVerbose) {
                SimulateDocumentVerboseResult expectedSimulateDocumentVerboseResult =
                        (SimulateDocumentVerboseResult) expectedResultIterator.next();
                assertThat(result, instanceOf(SimulateDocumentVerboseResult.class));
                SimulateDocumentVerboseResult simulateDocumentVerboseResult = (SimulateDocumentVerboseResult) result;
                assertThat(simulateDocumentVerboseResult.getProcessorResults().size(),
                        equalTo(expectedSimulateDocumentVerboseResult.getProcessorResults().size()));
                Iterator<SimulateProcessorResult> expectedProcessorResultIterator =
                        expectedSimulateDocumentVerboseResult.getProcessorResults().iterator();
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
                    assertIngestDocument(simulateDocumentBaseResult.getIngestDocument(),
                            expectedSimulateDocumentBaseResult.getIngestDocument());
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

    static SimulatePipelineResponse createInstance(String pipelineId, boolean isVerbose, boolean withFailure) {
        int numResults = randomIntBetween(1, 5);
        List<SimulateDocumentResult> results = new ArrayList<>(numResults);
        for (int i = 0; i < numResults; i++) {
            if (isVerbose) {
                results.add(
                    SimulateDocumentVerboseResultTests.createTestInstance(withFailure)
                );
            } else {
                results.add(
                    SimulateDocumentBaseResultTests.createTestInstance(withFailure && randomBoolean())
                );
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
                assertThat(response.getResults().get(i), instanceOf(SimulateDocumentVerboseResult.class));
                assertThat(parsedResponse.getResults().get(i), instanceOf(SimulateDocumentVerboseResult.class));
                SimulateDocumentVerboseResult responseResult = (SimulateDocumentVerboseResult)response.getResults().get(i);
                SimulateDocumentVerboseResult parsedResult = (SimulateDocumentVerboseResult)parsedResponse.getResults().get(i);
                SimulateDocumentVerboseResultTests.assertEqualDocs(responseResult, parsedResult);
            } else {
                assertThat(response.getResults().get(i), instanceOf(SimulateDocumentBaseResult.class));
                assertThat(parsedResponse.getResults().get(i), instanceOf(SimulateDocumentBaseResult.class));
                SimulateDocumentBaseResult responseResult = (SimulateDocumentBaseResult)response.getResults().get(i);
                SimulateDocumentBaseResult parsedResult = (SimulateDocumentBaseResult)parsedResponse.getResults().get(i);
                SimulateDocumentBaseResultTests.assertEqualDocs(responseResult, parsedResult);
            }
        }
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // We cannot have random fields in the _source field and _ingest field
        return field ->
            field.contains(
                new StringJoiner(".")
                    .add(WriteableIngestDocument.DOC_FIELD)
                    .add(WriteableIngestDocument.SOURCE_FIELD).toString()
            ) ||
                field.contains(
                    new StringJoiner(".")
                        .add(WriteableIngestDocument.DOC_FIELD)
                        .add(WriteableIngestDocument.INGEST_FIELD).toString()
                );
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
        AbstractXContentTestCase.testFromXContent(NUMBER_OF_TEST_RUNS, instanceSupplier, supportsUnknownFields(),
                getShuffleFieldsExceptions(), getRandomFieldsExcludeFilter(), this::createParser, this::doParseInstance,
                this::assertEqualInstances, assertToXContentEquivalence, getToXContentParams());
    }
}
