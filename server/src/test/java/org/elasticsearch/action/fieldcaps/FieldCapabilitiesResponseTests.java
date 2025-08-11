/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.ElasticsearchExceptionTests;
import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FieldCapabilitiesResponseTests extends AbstractWireSerializingTestCase<FieldCapabilitiesResponse> {

    @Override
    protected FieldCapabilitiesResponse createTestInstance() {
        FieldCapabilitiesResponse randomResponse;
        List<FieldCapabilitiesIndexResponse> responses = new ArrayList<>();
        int numResponse = randomIntBetween(0, 10);

        for (int i = 0; i < numResponse; i++) {
            responses.add(FieldCapabilitiesIndexResponseTests.randomIndexResponse());
        }
        randomResponse = new FieldCapabilitiesResponse(responses, Collections.emptyList());
        return randomResponse;
    }

    @Override
    protected Writeable.Reader<FieldCapabilitiesResponse> instanceReader() {
        return FieldCapabilitiesResponse::new;
    }

    public static IndexFieldCapabilities randomFieldCaps(String fieldName) {
        Map<String, String> meta;
        switch (randomInt(2)) {
            case 0:
                meta = Collections.emptyMap();
                break;
            case 1:
                meta = Collections.singletonMap("key", "value");
                break;
            default:
                meta = new HashMap<>();
                meta.put("key1", "value1");
                meta.put("key2", "value2");
                break;
        }

        return new IndexFieldCapabilities(
            fieldName,
            randomAlphaOfLengthBetween(5, 20),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            meta
        );
    }

    @Override
    protected FieldCapabilitiesResponse mutateInstance(FieldCapabilitiesResponse response) {
        Map<String, Map<String, FieldCapabilities>> mutatedResponses = new HashMap<>(response.get());

        int mutation = response.get().isEmpty() ? 0 : randomIntBetween(0, 2);

        switch (mutation) {
            case 0:
                String toAdd = randomAlphaOfLength(10);
                mutatedResponses.put(
                    toAdd,
                    Collections.singletonMap(randomAlphaOfLength(10), FieldCapabilitiesTests.randomFieldCaps(toAdd))
                );
                break;
            case 1:
                String toRemove = randomFrom(mutatedResponses.keySet());
                mutatedResponses.remove(toRemove);
                break;
            case 2:
                String toReplace = randomFrom(mutatedResponses.keySet());
                mutatedResponses.put(
                    toReplace,
                    Collections.singletonMap(randomAlphaOfLength(10), FieldCapabilitiesTests.randomFieldCaps(toReplace))
                );
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

    /**
     * check that failure serialization between different minor versions after 7.13 works
     */
    public void testMixedVersionFailureSerialization_post_7_13() throws IOException {
        FieldCapabilitiesResponse original = createResponseWithFailures();
        FieldCapabilitiesResponse deserialized;
        Version outVersion = VersionUtils.randomVersionBetween(random(), Version.V_7_13_0, Version.CURRENT);
        Version inVersion = VersionUtils.randomVersionBetween(random(), Version.V_7_13_0, Version.CURRENT);
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setVersion(outVersion);
            original.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), getNamedWriteableRegistry())) {

                in.setVersion(inVersion);
                deserialized = new FieldCapabilitiesResponse(in);
                assertEquals(-1, in.read());
            }
        }
        assertThat(deserialized.getIndices(), Matchers.equalTo(original.getIndices()));

        // only match size of failure list and indices, most exceptions don't support 'equals'
        List<FieldCapabilitiesFailure> deserializedFailures = deserialized.getFailures();
        assertEquals(deserializedFailures.size(), original.getFailures().size());
        int i = 0;
        for (FieldCapabilitiesFailure originalFailure : original.getFailures()) {
            FieldCapabilitiesFailure deserializedFaliure = deserializedFailures.get(i);
            assertThat(deserializedFaliure.getIndices(), Matchers.equalTo(originalFailure.getIndices()));
            i++;
        }
    }

    /**
     * check that failure serialization to minor versions before 7.13 works without serializing the failures part
     */
    public void testSerialization_pre_7_13() throws IOException {
        FieldCapabilitiesResponse original = createResponseWithFailures();
        FieldCapabilitiesResponse deserialized;
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_7_6_0, VersionUtils.getPreviousVersion(Version.V_7_13_0));
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setVersion(version);
            original.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), getNamedWriteableRegistry())) {
                in.setVersion(version);
                deserialized = new FieldCapabilitiesResponse(in);
                assertEquals(-1, in.read());
            }
        }
        assertThat(deserialized.getIndices(), Matchers.equalTo(original.getIndices()));

        // only match size of failure list and indices, most exceptions don't support 'equals'
        assertEquals(0, deserialized.getFailures().size());
    }

    public void testReadFromPre7_13() throws IOException {
        String msg = "AQpzb21lLWluZGV4AgdmaWVsZC0xAQdrZXl3b3JkB2ZpZWxkLTEHa2V5d29yZAEBAQABAAEAAAdmaWVsZC0y"
            + "AgdrZXl3b3JkB2ZpZWxkLTIHa2V5d29yZAEBAQABAAEAAARsb25nB2ZpZWxkLTIEbG9uZwEBAQABAAEAAAAAAAA=";
        try (StreamInput in = StreamInput.wrap(java.util.Base64.getDecoder().decode(msg))) {
            // minimum version set to 7.6 because the nested FieldCapabilities had another serialization change there
            in.setVersion(VersionUtils.randomVersionBetween(random(), Version.V_7_6_0, VersionUtils.getPreviousVersion(Version.V_7_13_0)));
            FieldCapabilitiesResponse deserialized = new FieldCapabilitiesResponse(in);
            assertArrayEquals(new String[] { "some-index" }, deserialized.getIndices());
            assertEquals(2, deserialized.get().size());
            assertNotNull(deserialized.get().get("field-1").get("keyword"));
            assertNotNull(deserialized.get().get("field-2").get("keyword"));
            assertEquals(0, deserialized.getIndexResponses().size());
            assertEquals(0, deserialized.getFailures().size());
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
                failures.add(new FieldCapabilitiesFailure(new String[] { index }, ElasticsearchExceptionTests.randomExceptions().v2()));
            } else {
                failures.get(failures.size() - 1).addIndex(index);
            }
        }
        return new FieldCapabilitiesResponse(indices, Collections.emptyMap(), failures);
    }
}
