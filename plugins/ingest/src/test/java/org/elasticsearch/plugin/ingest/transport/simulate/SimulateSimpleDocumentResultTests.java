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
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class SimulateSimpleDocumentResultTests extends ESTestCase {
    private Data data;

    @Before
    public void setup() {
        data = new Data("_index", "_type", "_id", Collections.singletonMap("foo", "bar"));
    }

    public void testEquals() {
        SimulateDocumentResult result = new SimulateSimpleDocumentResult(data);
        SimulateDocumentResult otherResult = new SimulateSimpleDocumentResult(data);
        assertThat(result, equalTo(otherResult));
    }

    public void testNotEqualsDiffData() {
        Data otherData = new Data(data.getIndex() + "foo", data.getType(), data.getId(), data.getDocument());
        SimulateDocumentResult result = new SimulateSimpleDocumentResult(data);
        SimulateDocumentResult otherResult = new SimulateSimpleDocumentResult(otherData);
        assertThat(result, not(equalTo(otherResult)));
    }

    public void testStreamable() throws IOException {
        SimulateDocumentResult result = new SimulateSimpleDocumentResult(data);

        BytesStreamOutput out = new BytesStreamOutput();
        result.writeTo(out);

        StreamInput streamInput = StreamInput.wrap(out.bytes());
        SimulateDocumentResult otherResult = new SimulateSimpleDocumentResult();
        otherResult.readFrom(streamInput);

        assertThat(result, equalTo(otherResult));
    }
}
