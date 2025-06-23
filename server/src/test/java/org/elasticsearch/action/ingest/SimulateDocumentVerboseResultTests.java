/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.ingest;

import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class SimulateDocumentVerboseResultTests extends AbstractXContentTestCase<SimulateDocumentVerboseResult> {

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<SimulateDocumentVerboseResult, Void> PARSER = new ConstructingObjectParser<>(
        "simulate_document_verbose_result",
        true,
        a -> new SimulateDocumentVerboseResult((List<SimulateProcessorResult>) a[0])
    );
    static {
        PARSER.declareObjectArray(
            constructorArg(),
            SimulateProcessorResult.PARSER,
            new ParseField(SimulateDocumentVerboseResult.PROCESSOR_RESULT_FIELD)
        );
    }

    static SimulateDocumentVerboseResult createTestInstance(boolean withFailures) {
        int numDocs = randomIntBetween(0, 5);
        List<SimulateProcessorResult> results = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            boolean isSuccessful = (withFailures && randomBoolean()) == false;
            boolean isIgnoredError = withFailures && randomBoolean();
            boolean hasCondition = withFailures && randomBoolean();
            results.add(SimulateProcessorResultTests.createTestInstance(isSuccessful, isIgnoredError, hasCondition));
        }
        return new SimulateDocumentVerboseResult(results);
    }

    private static SimulateDocumentVerboseResult createTestInstanceWithFailures() {
        return createTestInstance(true);
    }

    @Override
    protected SimulateDocumentVerboseResult createTestInstance() {
        return createTestInstance(false);
    }

    @Override
    protected SimulateDocumentVerboseResult doParseInstance(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    static void assertEqualDocs(SimulateDocumentVerboseResult response, SimulateDocumentVerboseResult parsedResponse) {
        assertEquals(response.getProcessorResults().size(), parsedResponse.getProcessorResults().size());
        for (int i = 0; i < response.getProcessorResults().size(); i++) {
            SimulateProcessorResultTests.assertEqualProcessorResults(
                response.getProcessorResults().get(i),
                parsedResponse.getProcessorResults().get(i)
            );
        }
    }

    @Override
    protected void assertEqualInstances(SimulateDocumentVerboseResult response, SimulateDocumentVerboseResult parsedResponse) {
        assertEqualDocs(response, parsedResponse);
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // We cannot have random fields in the _source field and _ingest field
        return field -> field.contains(
            new StringJoiner(".").add(WriteableIngestDocument.DOC_FIELD).add(WriteableIngestDocument.SOURCE_FIELD).toString()
        )
            || field.contains(
                new StringJoiner(".").add(WriteableIngestDocument.DOC_FIELD).add(WriteableIngestDocument.INGEST_FIELD).toString()
            );
    }

    /**
     * Test parsing {@link SimulateDocumentVerboseResult} with inner failures as they don't support asserting on xcontent
     * equivalence, given that exceptions are not parsed back as the same original class. We run the usual
     * {@link AbstractXContentTestCase#testFromXContent()} without failures, and this other test with failures where we
     * disable asserting on xcontent equivalence at the end.
     */
    public void testFromXContentWithFailures() throws IOException {
        Supplier<SimulateDocumentVerboseResult> instanceSupplier = SimulateDocumentVerboseResultTests::createTestInstanceWithFailures;
        // exceptions are not of the same type whenever parsed back
        boolean assertToXContentEquivalence = false;
        AbstractXContentTestCase.testFromXContent(
            NUMBER_OF_TEST_RUNS,
            instanceSupplier,
            supportsUnknownFields(),
            getShuffleFieldsExceptions(),
            getRandomFieldsExcludeFilter(),
            this::createParser,
            this::doParseInstance,
            this::assertEqualInstances,
            assertToXContentEquivalence,
            getToXContentParams()
        );
    }
}
