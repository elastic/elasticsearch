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
import static org.elasticsearch.action.ingest.WriteableIngestDocumentTests.createRandomIngestDoc;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class SimulateProcessorResultTests extends AbstractXContentTestCase<SimulateProcessorResult> {

    public void testSerialization() throws IOException {
        boolean isSuccessful = randomBoolean();
        boolean isIgnoredException = randomBoolean();
        SimulateProcessorResult simulateProcessorResult = createTestInstance(isSuccessful, isIgnoredException);

        BytesStreamOutput out = new BytesStreamOutput();
        simulateProcessorResult.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        SimulateProcessorResult otherSimulateProcessorResult = new SimulateProcessorResult(streamInput);
        assertThat(otherSimulateProcessorResult.getProcessorTag(), equalTo(simulateProcessorResult.getProcessorTag()));
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
                                                                boolean isIgnoredException) {
        String processorTag = randomAlphaOfLengthBetween(1, 10);
        SimulateProcessorResult simulateProcessorResult;
        if (isSuccessful) {
            IngestDocument ingestDocument = createRandomIngestDoc();
            if (isIgnoredException) {
                simulateProcessorResult = new SimulateProcessorResult(processorTag, ingestDocument, new IllegalArgumentException("test"));
            } else {
                simulateProcessorResult = new SimulateProcessorResult(processorTag, ingestDocument);
            }
        } else {
            simulateProcessorResult = new SimulateProcessorResult(processorTag, new IllegalArgumentException("test"));
        }
        return simulateProcessorResult;
    }

    private static SimulateProcessorResult createTestInstanceWithFailures() {
        boolean isSuccessful = randomBoolean();
        boolean isIgnoredException = randomBoolean();
        return createTestInstance(isSuccessful, isIgnoredException);
    }

    @Override
    protected SimulateProcessorResult createTestInstance() {
        // we test failures separately since comparing XContent is not possible with failures
        return createTestInstance(true, false);
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
}
