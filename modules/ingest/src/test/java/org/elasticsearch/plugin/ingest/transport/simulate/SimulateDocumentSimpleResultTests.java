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

package org.elasticsearch.plugin.ingest.transport.simulate;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

public class SimulateDocumentSimpleResultTests extends ESTestCase {

    public void testSerialization() throws IOException {
        boolean isFailure = randomBoolean();
        SimulateDocumentSimpleResult simulateDocumentSimpleResult;
        if (isFailure) {
            simulateDocumentSimpleResult = new SimulateDocumentSimpleResult(new IllegalArgumentException("test"));
        } else {
            IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
            simulateDocumentSimpleResult = new SimulateDocumentSimpleResult(ingestDocument);
        }

        BytesStreamOutput out = new BytesStreamOutput();
        simulateDocumentSimpleResult.writeTo(out);
        StreamInput streamInput = StreamInput.wrap(out.bytes());
        SimulateDocumentSimpleResult otherSimulateDocumentSimpleResult = SimulateDocumentSimpleResult.readSimulateDocumentSimpleResult(streamInput);

        assertThat(otherSimulateDocumentSimpleResult.getIngestDocument(), equalTo(simulateDocumentSimpleResult.getIngestDocument()));
        if (isFailure) {
            assertThat(otherSimulateDocumentSimpleResult.getFailure(), instanceOf(IllegalArgumentException.class));
            IllegalArgumentException e = (IllegalArgumentException) otherSimulateDocumentSimpleResult.getFailure();
            assertThat(e.getMessage(), equalTo("test"));
        }
    }
}
