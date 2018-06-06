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
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

public class SimulateDocumentBaseResultTests extends AbstractXContentTestCase<SimulateDocumentBaseResult> {

    public void testSerialization() throws IOException {
        boolean isFailure = randomBoolean();
        SimulateDocumentBaseResult simulateDocumentBaseResult = createTestInstance(isFailure, true);

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

    protected SimulateDocumentBaseResult createTestInstance(boolean isFailure, boolean withByteArray) {
        SimulateDocumentBaseResult simulateDocumentBaseResult;
        if (isFailure) {
            simulateDocumentBaseResult = new SimulateDocumentBaseResult(new IllegalArgumentException("test"));
        } else {
            IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), withByteArray);
            simulateDocumentBaseResult = new SimulateDocumentBaseResult(ingestDocument);
        }
        return simulateDocumentBaseResult;
    }

    @Override
    protected SimulateDocumentBaseResult createTestInstance() {
        return createTestInstance(randomBoolean(), false);
    }

    @Override
    protected SimulateDocumentBaseResult doParseInstance(XContentParser parser) {
        return SimulateDocumentBaseResult.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
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

    @Override
    protected boolean assertToXContentEquivalence() {
        return false;
    }
}
