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
import org.elasticsearch.ingest.Data;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class SimulatePipelineResponseTests extends ESTestCase {
    private Data data;
    private SimulateDocumentResult documentResult;
    private SimulatePipelineResponse response;

    @Before
    public void setup() {
        data = new Data("_index", "_type", "_id", Collections.singletonMap("foo", "bar"));
        documentResult = new SimulateSimpleDocumentResult(data);
        response = new SimulatePipelineResponse("_id", Collections.singletonList(documentResult));
    }

    public void testEquals() {
        SimulatePipelineResponse otherResponse = new SimulatePipelineResponse("_id", Collections.singletonList(documentResult));
        assertThat(response, equalTo(otherResponse));
    }

    public void testNotEqualsId() {
        SimulatePipelineResponse otherResponse = new SimulatePipelineResponse(response.getPipelineId() + "foo", response.getResults());
        assertThat(response, not(equalTo(otherResponse)));
    }

    public void testNotEqualsResults() {
        SimulatePipelineResponse otherResponse = new SimulatePipelineResponse(response.getPipelineId(), Arrays.asList(documentResult, documentResult));
        assertThat(response, not(equalTo(otherResponse)));
    }

    public void testStreamable() throws IOException {
        List<SimulateDocumentResult> results = Arrays.asList(
                new SimulateSimpleDocumentResult(data),
                new SimulateFailedDocumentResult(new IllegalArgumentException("foo")),
                new SimulateVerboseDocumentResult(Collections.singletonList(new SimulateProcessorResult("pid", data)))
        );

        response = new SimulatePipelineResponse("_id", results);
        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);

        StreamInput streamInput = StreamInput.wrap(out.bytes());
        SimulatePipelineResponse otherResponse = new SimulatePipelineResponse();
        otherResponse.readFrom(streamInput);

        assertThat(response, equalTo(otherResponse));
    }
}
