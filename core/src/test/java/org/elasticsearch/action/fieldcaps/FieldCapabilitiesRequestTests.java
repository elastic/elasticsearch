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

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class FieldCapabilitiesRequestTests extends ESTestCase {
    private FieldCapabilitiesRequest randomRequest() {
        FieldCapabilitiesRequest request =  new FieldCapabilitiesRequest();
        int size = randomIntBetween(1, 20);
        String[] randomFields = new String[size];
        for (int i = 0; i < size; i++) {
            randomFields[i] = randomAsciiOfLengthBetween(5, 10);
        }
        request.fields(randomFields);
        return request;
    }

    public void testFieldCapsRequestSerialization() throws IOException {
        for (int i = 0; i < 20; i++) {
            FieldCapabilitiesRequest request = randomRequest();
            BytesStreamOutput output = new BytesStreamOutput();
            request.writeTo(output);
            output.flush();
            StreamInput input = output.bytes().streamInput();
            FieldCapabilitiesRequest deserialized = new FieldCapabilitiesRequest();
            deserialized.readFrom(input);
            assertEquals(deserialized, request);
            assertEquals(deserialized.hashCode(), request.hashCode());
        }
    }
}
