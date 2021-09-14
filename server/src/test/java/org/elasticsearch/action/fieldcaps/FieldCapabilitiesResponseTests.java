/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.ElasticsearchExceptionTests;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomAsciiLettersOfLength;

public class FieldCapabilitiesResponseTests extends AbstractWireSerializingTestCase<FieldCapabilitiesResponse> {

    @Override
    protected FieldCapabilitiesResponse createTestInstance() {
        FieldCapabilitiesResponse randomResponse;
        List<FieldCapabilitiesIndexResponse> responses = new ArrayList<>();
        int numResponse = randomIntBetween(0, 10);

        for (int i = 0; i < numResponse; i++) {
            responses.add(createRandomIndexResponse());
        }
        randomResponse = new FieldCapabilitiesResponse(responses, Collections.emptyList());
        return randomResponse;
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
            randomBoolean(), randomBoolean(), randomBoolean(), meta);
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
        return new FieldCapabilitiesResponse(null, mutatedResponses, Collections.emptyList());
    }

    public void testFailureSerialization() throws IOException {
        FieldCapabilitiesResponse randomResponse = createResponseWithFailures();
        FieldCapabilitiesResponse deserialized = copyInstance(randomResponse);
        assertThat(deserialized.getIndices(), Matchers.equalTo(randomResponse.getIndices()));
        // only match size of failure list and indices, most exceptions don't support 'equals'
        List<FieldCapabilitiesFailure> deserializedFailures = deserialized.getFailures();
        assertEquals(deserializedFailures.size(), randomResponse.getFailures().size());
        int i = 0;
        for (FieldCapabilitiesFailure originalFailure : randomResponse.getFailures()) {
            FieldCapabilitiesFailure deserializedFaliure = deserializedFailures.get(i);
            assertThat(deserializedFaliure.getIndices(), Matchers.equalTo(originalFailure.getIndices()));
            i++;
        }
    }

    public void testFailureParsing() throws IOException {
        FieldCapabilitiesResponse randomResponse = createResponseWithFailures();
        boolean humanReadable = randomBoolean();
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference originalBytes = toShuffledXContent(randomResponse, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);
        FieldCapabilitiesResponse parsedResponse;
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            parsedResponse = FieldCapabilitiesResponse.fromXContent(parser);
            assertNull(parser.nextToken());
        }
        assertNotSame(parsedResponse, randomResponse);
        assertThat(parsedResponse.getIndices(), Matchers.equalTo(randomResponse.getIndices()));
        // only match size of failure list and indices, most exceptions don't support 'equals'
        List<FieldCapabilitiesFailure> deserializedFailures = parsedResponse.getFailures();
        assertEquals(deserializedFailures.size(), randomResponse.getFailures().size());
        int i = 0;
        for (FieldCapabilitiesFailure originalFailure : randomResponse.getFailures()) {
            FieldCapabilitiesFailure deserializedFaliure = deserializedFailures.get(i);
            assertThat(deserializedFaliure.getIndices(), Matchers.equalTo(originalFailure.getIndices()));
            i++;
        }
    }

    private FieldCapabilitiesResponse createResponseWithFailures() {
        String[] indices = randomArray(randomIntBetween(1, 5), String[]::new, () -> randomAlphaOfLength(5));
        List<FieldCapabilitiesFailure> failures = new ArrayList<>();
        for (String index : indices) {
            if (randomBoolean() || failures.size() == 0) {
                failures.add(new FieldCapabilitiesFailure(new String[] {index}, ElasticsearchExceptionTests.randomExceptions().v2()));
            } else {
                failures.get(failures.size() - 1).addIndex(index);
            }
        }
        return new FieldCapabilitiesResponse(indices, Collections.emptyMap(), failures);
    }
}
