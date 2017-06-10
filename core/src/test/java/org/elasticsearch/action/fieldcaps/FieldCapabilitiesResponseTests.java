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
import java.util.HashMap;
import java.util.Map;

public class FieldCapabilitiesResponseTests extends ESTestCase {
    private FieldCapabilitiesResponse randomResponse() {
        Map<String, Map<String, FieldCapabilities> > fieldMap = new HashMap<> ();
        int numFields = randomInt(10);
        for (int i = 0; i < numFields; i++) {
            String fieldName = randomAlphaOfLengthBetween(5, 10);
            int numIndices = randomIntBetween(1, 5);
            Map<String, FieldCapabilities> indexFieldMap = new HashMap<> ();
            for (int j = 0; j < numIndices; j++) {
                String index = randomAlphaOfLengthBetween(10, 20);
                indexFieldMap.put(index, FieldCapabilitiesTests.randomFieldCaps());
            }
            fieldMap.put(fieldName, indexFieldMap);
        }
        return new FieldCapabilitiesResponse(fieldMap);
    }

    public void testSerialization() throws IOException {
        for (int i = 0; i < 20; i++) {
            FieldCapabilitiesResponse response = randomResponse();
            BytesStreamOutput output = new BytesStreamOutput();
            response.writeTo(output);
            output.flush();
            StreamInput input = output.bytes().streamInput();
            FieldCapabilitiesResponse deserialized = new FieldCapabilitiesResponse();
            deserialized.readFrom(input);
            assertEquals(deserialized, response);
            assertEquals(deserialized.hashCode(), response.hashCode());
        }
    }
}
