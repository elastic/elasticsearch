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
