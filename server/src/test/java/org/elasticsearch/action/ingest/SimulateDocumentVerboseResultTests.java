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

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.util.ArrayList;
import java.util.List;

public class SimulateDocumentVerboseResultTests extends AbstractXContentTestCase<SimulateDocumentVerboseResult> {

    @Override
    protected SimulateDocumentVerboseResult createTestInstance() {
        int numDocs = randomIntBetween(0, 10);
        List<SimulateProcessorResult> results = new ArrayList<>();
        for (int i = 0; i<numDocs; i++) {
            results.add(
                SimulateProcessorResultTests.createTestInstance(randomBoolean(), randomBoolean())
            );
        }
        return new SimulateDocumentVerboseResult(results);
    }

    @Override
    protected SimulateDocumentVerboseResult doParseInstance(XContentParser parser) {
        return SimulateDocumentVerboseResult.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    protected static void assertEqualDocs(SimulateDocumentVerboseResult response,
                                          SimulateDocumentVerboseResult parsedResponse) {
        assertEquals(response.getProcessorResults().size(), parsedResponse.getProcessorResults().size());
        for (int i=0; i < response.getProcessorResults().size(); i++) {
            SimulateProcessorResultTests.assertEqualProcessorResults(
                response.getProcessorResults().get(i),
                parsedResponse.getProcessorResults().get(i)
            );
        }
    }

    @Override
    protected void assertEqualInstances(SimulateDocumentVerboseResult response,
                                        SimulateDocumentVerboseResult parsedResponse) {
        assertEqualDocs(response, parsedResponse);
    }

    @Override
    protected boolean assertToXContentEquivalence() {
        return false;
    }
}
