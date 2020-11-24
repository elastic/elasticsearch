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

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomAsciiLettersOfLength;

public class FieldCapabilitiesResponseTests extends AbstractWireSerializingTestCase<FieldCapabilitiesResponse> {

    @Override
    protected FieldCapabilitiesResponse createTestInstance() {
        List<FieldCapabilitiesIndexResponse> responses = new ArrayList<>();
        int numResponse = randomIntBetween(0, 10);

        for (int i = 0; i < numResponse; i++) {
            responses.add(createRandomIndexResponse());
        }
        return new FieldCapabilitiesResponse(responses);
    }

    @Override
    protected Writeable.Reader<FieldCapabilitiesResponse> instanceReader() {
        return FieldCapabilitiesResponse::new;
    }

    private FieldCapabilitiesIndexResponse createRandomIndexResponse() {
        Map<String, IndexFieldCapabilities> responses = new HashMap<>();

        String[] fields = generateRandomStringArray(5, 10, false, true);
        assertNotNull(fields);

        for (String field : fields) {
            responses.put(field, randomFieldCaps(field));
        }
        return new FieldCapabilitiesIndexResponse(randomAsciiLettersOfLength(10), responses, randomBoolean());
    }

    private static IndexFieldCapabilities randomFieldCaps(String fieldName) {
        Map<String, String> meta;
        switch (randomInt(2)) {
            case 0:
                meta = Collections.emptyMap();
                break;
            case 1:
                meta = Map.of("key", "value");
                break;
            default:
                meta = Map.of("key1", "value1", "key2", "value2");
                break;
        }

        return new IndexFieldCapabilities(fieldName, randomAlphaOfLengthBetween(5, 20),
            randomBoolean(), randomBoolean(), meta);
    }

    @Override
    protected FieldCapabilitiesResponse mutateInstance(FieldCapabilitiesResponse response) {
        Map<String, Map<String, FieldCapabilities>> mutatedResponses = new HashMap<>(response.get());

        int mutation = response.get().isEmpty() ? 0 : randomIntBetween(0, 2);

        switch (mutation) {
            case 0:
                String toAdd = randomAlphaOfLength(10);
                mutatedResponses.put(toAdd, Collections.singletonMap(
                    randomAlphaOfLength(10),
                    FieldCapabilitiesTests.randomFieldCaps(toAdd)));
                break;
            case 1:
                String toRemove = randomFrom(mutatedResponses.keySet());
                mutatedResponses.remove(toRemove);
                break;
            case 2:
                String toReplace = randomFrom(mutatedResponses.keySet());
                mutatedResponses.put(toReplace, Collections.singletonMap(
                    randomAlphaOfLength(10),
                    FieldCapabilitiesTests.randomFieldCaps(toReplace)));
                break;
        }
        return new FieldCapabilitiesResponse(null, mutatedResponses);
    }
}
