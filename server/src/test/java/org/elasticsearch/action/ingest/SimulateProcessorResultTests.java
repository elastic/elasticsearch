/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.ingest;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.StringJoiner;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.action.ingest.WriteableIngestDocumentTests.createRandomIngestDoc;
import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class SimulateProcessorResultTests extends AbstractXContentTestCase<SimulateProcessorResult> {

    public void testSerialization() throws IOException {
        boolean isSuccessful = randomBoolean();
        boolean isIgnoredException = randomBoolean();
        boolean hasCondition = randomBoolean();
        SimulateProcessorResult simulateProcessorResult = createTestInstance(isSuccessful, isIgnoredException, hasCondition);

        BytesStreamOutput out = new BytesStreamOutput();
        simulateProcessorResult.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        SimulateProcessorResult otherSimulateProcessorResult = new SimulateProcessorResult(streamInput);
        assertThat(otherSimulateProcessorResult.getProcessorTag(), equalTo(simulateProcessorResult.getProcessorTag()));
        assertThat(otherSimulateProcessorResult.getDescription(), equalTo(simulateProcessorResult.getDescription()));
        if (isSuccessful) {
            assertIngestDocument(otherSimulateProcessorResult.getIngestDocument(), simulateProcessorResult.getIngestDocument());
            if (isIgnoredException) {
                assertThat(otherSimulateProcessorResult.getFailure(), instanceOf(IllegalArgumentException.class));
                IllegalArgumentException e = (IllegalArgumentException) otherSimulateProcessorResult.getFailure();
                assertThat(e.getMessage(), equalTo("test"));
            } else {
                assertThat(otherSimulateProcessorResult.getFailure(), nullValue());
            }
        } else {
            assertThat(otherSimulateProcessorResult.getIngestDocument(), is(nullValue()));
            assertThat(otherSimulateProcessorResult.getFailure(), instanceOf(IllegalArgumentException.class));
            IllegalArgumentException e = (IllegalArgumentException) otherSimulateProcessorResult.getFailure();
            assertThat(e.getMessage(), equalTo("test"));
        }
    }

    static SimulateProcessorResult createTestInstance(boolean isSuccessful,
                                                                boolean isIgnoredException, boolean hasCondition) {
        String type = randomAlphaOfLengthBetween(1, 10);
        String processorTag = randomAlphaOfLengthBetween(1, 10);
        String description = randomAlphaOfLengthBetween(1, 10);
        Tuple<String, Boolean> conditionWithResult = hasCondition ? new Tuple<>(randomAlphaOfLengthBetween(1, 10), randomBoolean()) : null;
        SimulateProcessorResult simulateProcessorResult;
        if (isSuccessful) {
            IngestDocument ingestDocument = createRandomIngestDoc();
            if (isIgnoredException) {
                simulateProcessorResult = new SimulateProcessorResult(type, processorTag, description, ingestDocument,
                    new IllegalArgumentException("test"), conditionWithResult);
            } else {
                simulateProcessorResult = new SimulateProcessorResult(type, processorTag, description, ingestDocument, conditionWithResult);
            }
        } else {
            simulateProcessorResult = new SimulateProcessorResult(type, processorTag, description,
                new IllegalArgumentException("test"), conditionWithResult);
        }
        return simulateProcessorResult;
    }

    private static SimulateProcessorResult createTestInstanceWithFailures() {
        boolean isSuccessful = randomBoolean();
        boolean isIgnoredException = randomBoolean();
        boolean hasCondition = randomBoolean();
        return createTestInstance(isSuccessful, isIgnoredException, hasCondition);
    }

    @Override
    protected SimulateProcessorResult createTestInstance() {
        // we test failures separately since comparing XContent is not possible with failures
        return createTestInstance(true, false, true);
    }

    @Override
    protected SimulateProcessorResult doParseInstance(XContentParser parser) {
        return SimulateProcessorResult.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // We cannot have random fields in the _source field and _ingest field
        return field ->
            field.startsWith(
                new StringJoiner(".")
                    .add(WriteableIngestDocument.DOC_FIELD)
                    .add(WriteableIngestDocument.SOURCE_FIELD).toString()
            ) ||
                field.startsWith(
                    new StringJoiner(".")
                        .add(WriteableIngestDocument.DOC_FIELD)
                        .add(WriteableIngestDocument.INGEST_FIELD).toString()
                );
    }

    static void assertEqualProcessorResults(SimulateProcessorResult response,
                                                      SimulateProcessorResult parsedResponse) {
        assertEquals(response.getProcessorTag(), parsedResponse.getProcessorTag());
        assertEquals(response.getIngestDocument(), parsedResponse.getIngestDocument());
        if (response.getFailure() != null ) {
            assertNotNull(parsedResponse.getFailure());
            assertThat(
                parsedResponse.getFailure().getMessage(),
                containsString(response.getFailure().getMessage())
            );
        } else {
            assertNull(parsedResponse.getFailure());
        }
    }

    @Override
    protected void assertEqualInstances(SimulateProcessorResult response, SimulateProcessorResult parsedResponse) {
        assertEqualProcessorResults(response, parsedResponse);
    }

    /**
     * Test parsing {@link SimulateProcessorResult} with inner failures as they don't support asserting on xcontent equivalence, given that
     * exceptions are not parsed back as the same original class. We run the usual {@link AbstractXContentTestCase#testFromXContent()}
     * without failures, and this other test with failures where we disable asserting on xcontent equivalence at the end.
     */
    public void testFromXContentWithFailures() throws IOException {
        Supplier<SimulateProcessorResult> instanceSupplier = SimulateProcessorResultTests::createTestInstanceWithFailures;
        //with random fields insertion in the inner exceptions, some random stuff may be parsed back as metadata,
        //but that does not bother our assertions, as we only want to test that we don't break.
        boolean supportsUnknownFields = true;
        //exceptions are not of the same type whenever parsed back
        boolean assertToXContentEquivalence = false;
        AbstractXContentTestCase.testFromXContent(NUMBER_OF_TEST_RUNS, instanceSupplier, supportsUnknownFields,
            getShuffleFieldsExceptions(), getRandomFieldsExcludeFilter(), this::createParser, this::doParseInstance,
            this::assertEqualInstances, assertToXContentEquivalence, getToXContentParams());
    }

    public void testStatus(){
        SimulateProcessorResult result;
        // conditional returned false
        result = new SimulateProcessorResult(null, null, null, createRandomIngestDoc(), null,
            new Tuple<>(randomAlphaOfLengthBetween(1, 10), false));
        assertEquals(SimulateProcessorResult.Status.SKIPPED, result.getStatus("set"));

        // no ingest doc
        result = new SimulateProcessorResult(null, null, null, null, null, null);
        assertEquals(SimulateProcessorResult.Status.DROPPED, result.getStatus(null));

        // no ingest doc - as pipeline processor
        result = new SimulateProcessorResult(null, null, null, null, null, null);
        assertEquals(SimulateProcessorResult.Status.SUCCESS, result.getStatus("pipeline"));

        // failure
        result = new SimulateProcessorResult(null, null, null, null, new RuntimeException(""), null);
        assertEquals(SimulateProcessorResult.Status.ERROR, result.getStatus("rename"));

        // failure, but ignored
        result = new SimulateProcessorResult(null, null, null, createRandomIngestDoc(), new RuntimeException(""), null);
        assertEquals(SimulateProcessorResult.Status.ERROR_IGNORED, result.getStatus(""));

        //success - no conditional
        result = new SimulateProcessorResult(null, null, null, createRandomIngestDoc(), null, null);
        assertEquals(SimulateProcessorResult.Status.SUCCESS, result.getStatus(null));

        //success - conditional true
        result = new SimulateProcessorResult(null, null, null, createRandomIngestDoc(), null,
            new Tuple<>(randomAlphaOfLengthBetween(1, 10), true));
        assertEquals(SimulateProcessorResult.Status.SUCCESS, result.getStatus(null));
    }
}
