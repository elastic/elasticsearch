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
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.StringJoiner;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.elasticsearch.action.ingest.WriteableIngestDocumentTests.createRandomIngestDoc;

public class SimulateDocumentBaseResultTests extends AbstractXContentTestCase<SimulateDocumentBaseResult> {

    public void testSerialization() throws IOException {
        boolean isFailure = randomBoolean();
        SimulateDocumentBaseResult simulateDocumentBaseResult = createTestInstance(isFailure);

        BytesStreamOutput out = new BytesStreamOutput();
        simulateDocumentBaseResult.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        SimulateDocumentBaseResult otherSimulateDocumentBaseResult = new SimulateDocumentBaseResult(streamInput);

        if (isFailure) {
            assertThat(otherSimulateDocumentBaseResult.getIngestDocument(), equalTo(simulateDocumentBaseResult.getIngestDocument()));
            assertThat(otherSimulateDocumentBaseResult.getFailure(), instanceOf(IllegalArgumentException.class));
            IllegalArgumentException e = (IllegalArgumentException) otherSimulateDocumentBaseResult.getFailure();
            assertThat(e.getMessage(), equalTo("test"));
        } else {
            assertIngestDocument(otherSimulateDocumentBaseResult.getIngestDocument(), simulateDocumentBaseResult.getIngestDocument());
        }
    }

    static SimulateDocumentBaseResult createTestInstance(boolean isFailure) {
        SimulateDocumentBaseResult simulateDocumentBaseResult;
        if (isFailure) {
            simulateDocumentBaseResult = new SimulateDocumentBaseResult(new IllegalArgumentException("test"));
        } else {
            IngestDocument ingestDocument = createRandomIngestDoc();
            simulateDocumentBaseResult = new SimulateDocumentBaseResult(ingestDocument);
        }
        return simulateDocumentBaseResult;
    }

    private static SimulateDocumentBaseResult createTestInstanceWithFailures() {
        return createTestInstance(randomBoolean());
    }

    @Override
    protected SimulateDocumentBaseResult createTestInstance() {
        return createTestInstance(false);
    }

    @Override
    protected SimulateDocumentBaseResult doParseInstance(XContentParser parser) {
        return SimulateDocumentBaseResult.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
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

    public static void assertEqualDocs(SimulateDocumentBaseResult response, SimulateDocumentBaseResult parsedResponse) {
        assertEquals(response.getIngestDocument(), parsedResponse.getIngestDocument());
        if (response.getFailure() != null) {
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
    public void assertEqualInstances(SimulateDocumentBaseResult response, SimulateDocumentBaseResult parsedResponse) {
        assertEqualDocs(response, parsedResponse);
    }

    /**
     * Test parsing {@link SimulateDocumentBaseResult} with inner failures as they don't support asserting on xcontent
     * equivalence, given that exceptions are not parsed back as the same original class. We run the usual
     * {@link AbstractXContentTestCase#testFromXContent()} without failures, and this other test with failures where
     * we disable asserting on xcontent equivalence at the end.
     */
    public void testFromXContentWithFailures() throws IOException {
        Supplier<SimulateDocumentBaseResult> instanceSupplier = SimulateDocumentBaseResultTests::createTestInstanceWithFailures;
        //exceptions are not of the same type whenever parsed back
        boolean assertToXContentEquivalence = false;
        AbstractXContentTestCase.testFromXContent(NUMBER_OF_TEST_RUNS, instanceSupplier, supportsUnknownFields(),
            getShuffleFieldsExceptions(), getRandomFieldsExcludeFilter(), this::createParser, this::doParseInstance,
            this::assertEqualInstances, assertToXContentEquivalence, getToXContentParams());
    }
}
