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

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.ingest.IngestDocumentTests.assertIngestDocument;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;

public class SimulatePipelineRequestTests extends ESTestCase {

    public void testSerialization() throws IOException {
        SimulatePipelineRequest request = new SimulatePipelineRequest(new BytesArray(""));
        // Sometimes we set an id
        if (randomBoolean()) {
            request.setId(randomAsciiOfLengthBetween(1, 10));
        }

        // Sometimes we explicitly set a boolean (with whatever value)
        if (randomBoolean()) {
            request.setVerbose(randomBoolean());
        }

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        SimulatePipelineRequest otherRequest = new SimulatePipelineRequest();
        otherRequest.readFrom(streamInput);

        assertThat(otherRequest.getId(), equalTo(request.getId()));
        assertThat(otherRequest.isVerbose(), equalTo(request.isVerbose()));
    }
}
