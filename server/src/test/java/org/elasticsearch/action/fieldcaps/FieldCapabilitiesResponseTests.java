/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.ElasticsearchExceptionTests;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponseTests.randomIndexResponsesWithMappingHash;
import static org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponseTests.randomIndexResponsesWithoutMappingHash;
import static org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponseTests.randomMappingHashToIndices;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

public class FieldCapabilitiesResponseTests extends AbstractWireSerializingTestCase<FieldCapabilitiesResponse> {

    @Override
    protected FieldCapabilitiesResponse createTestInstance() {
        FieldCapabilitiesResponse randomResponse;
        List<FieldCapabilitiesIndexResponse> responses = new ArrayList<>();
        int numResponse = randomIntBetween(0, 10);
        for (int i = 0; i < numResponse; i++) {
            Map<String, IndexFieldCapabilities> fieldCaps = FieldCapabilitiesIndexResponseTests.randomFieldCaps();
            responses.add(new FieldCapabilitiesIndexResponse("index_" + i, null, fieldCaps, randomBoolean()));
        }
        randomResponse = new FieldCapabilitiesResponse(responses, Collections.emptyList());
        return randomResponse;
    }

    @Override
    protected Writeable.Reader<FieldCapabilitiesResponse> instanceReader() {
        return FieldCapabilitiesResponse::new;
    }

    @Override
    protected FieldCapabilitiesResponse mutateInstance(FieldCapabilitiesResponse response) {
        Map<String, Map<String, FieldCapabilities>> mutatedResponses = new HashMap<>(response.get());

        int mutation = response.get().isEmpty() ? 0 : randomIntBetween(0, 2);

        switch (mutation) {
            case 0 -> {
                String toAdd = randomAlphaOfLength(10);
                mutatedResponses.put(
                    toAdd,
                    Collections.singletonMap(randomAlphaOfLength(10), FieldCapabilitiesTests.randomFieldCaps(toAdd))
                );
            }
            case 1 -> {
                String toRemove = randomFrom(mutatedResponses.keySet());
                mutatedResponses.remove(toRemove);
            }
            case 2 -> {
                String toReplace = randomFrom(mutatedResponses.keySet());
                mutatedResponses.put(
                    toReplace,
                    Collections.singletonMap(randomAlphaOfLength(10), FieldCapabilitiesTests.randomFieldCaps(toReplace))
                );
            }
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
        BytesReference originalBytes = toShuffledXContent(
            ChunkedToXContent.wrapAsToXContent(randomResponse),
            xContentType,
            ToXContent.EMPTY_PARAMS,
            humanReadable
        );
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

    public static FieldCapabilitiesResponse createResponseWithFailures() {
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

    private static FieldCapabilitiesResponse randomCCSResponse(List<FieldCapabilitiesIndexResponse> indexResponses) {
        int numFailures = between(0, 4);
        List<FieldCapabilitiesFailure> failures = new ArrayList<>();
        for (int i = 0; i < numFailures; i++) {
            String index = "index_" + i;
            failures.add(new FieldCapabilitiesFailure(new String[] { index }, ElasticsearchExceptionTests.randomExceptions().v2()));
        }
        return new FieldCapabilitiesResponse(indexResponses, failures);
    }

    public void testSerializeCCSResponseBetweenNewClusters() throws Exception {
        Map<String, List<String>> mappingHashToIndices = randomMappingHashToIndices();
        List<FieldCapabilitiesIndexResponse> indexResponses = CollectionUtils.concatLists(
            randomIndexResponsesWithMappingHash(mappingHashToIndices),
            randomIndexResponsesWithoutMappingHash()
        );
        Randomness.shuffle(indexResponses);
        FieldCapabilitiesResponse inResponse = randomCCSResponse(indexResponses);
        final TransportVersion version = TransportVersionUtils.randomVersionBetween(
            random(),
            TransportVersion.V_8_2_0,
            TransportVersion.CURRENT
        );
        final FieldCapabilitiesResponse outResponse = copyInstance(inResponse, version);
        assertThat(
            outResponse.getFailures().stream().flatMap(f -> Arrays.stream(f.getIndices())).toList(),
            equalTo(inResponse.getFailures().stream().flatMap(f -> Arrays.stream(f.getIndices())).toList())
        );
        final List<FieldCapabilitiesIndexResponse> inList = inResponse.getIndexResponses();
        final List<FieldCapabilitiesIndexResponse> outList = outResponse.getIndexResponses();
        assertThat(outList, hasSize(inList.size()));
        assertThat(
            outList.stream().sorted(Comparator.comparing(FieldCapabilitiesIndexResponse::getIndexName)).toList(),
            equalTo(inList.stream().sorted(Comparator.comparing(FieldCapabilitiesIndexResponse::getIndexName)).toList())
        );
        Map<String, List<FieldCapabilitiesIndexResponse>> groupedResponses = outList.stream()
            .filter(r -> r.canMatch() && r.getIndexMappingHash() != null)
            .collect(Collectors.groupingBy(FieldCapabilitiesIndexResponse::getIndexMappingHash));
        assertThat(groupedResponses.keySet(), equalTo(mappingHashToIndices.keySet()));
        // Asserts responses of indices with the same mapping hash must be shared.
        for (Map.Entry<String, List<FieldCapabilitiesIndexResponse>> e : groupedResponses.entrySet()) {
            List<String> indices = mappingHashToIndices.get(e.getKey());
            List<FieldCapabilitiesIndexResponse> rs = e.getValue();
            assertThat(rs.stream().map(FieldCapabilitiesIndexResponse::getIndexName).sorted().toList(), equalTo(indices));
            for (FieldCapabilitiesIndexResponse r : rs) {
                assertTrue(r.canMatch());
                assertSame(r.get(), rs.get(0).get());
            }
        }
    }

    public void testSerializeCCSResponseBetweenOldClusters() throws IOException {
        TransportVersion minCompactVersion = TransportVersion.MINIMUM_COMPATIBLE;
        assertTrue("Remove this test once minCompactVersion >= 8.2.0", minCompactVersion.before(TransportVersion.V_8_2_0));
        List<FieldCapabilitiesIndexResponse> indexResponses = CollectionUtils.concatLists(
            randomIndexResponsesWithMappingHash(randomMappingHashToIndices()),
            randomIndexResponsesWithoutMappingHash()
        );
        Randomness.shuffle(indexResponses);
        FieldCapabilitiesResponse inResponse = randomCCSResponse(indexResponses);
        TransportVersion version = TransportVersionUtils.randomVersionBetween(
            random(),
            minCompactVersion,
            TransportVersionUtils.getPreviousVersion(TransportVersion.V_8_2_0)
        );
        final FieldCapabilitiesResponse outResponse = copyInstance(inResponse, version);
        assertThat(
            outResponse.getFailures().stream().flatMap(f -> Arrays.stream(f.getIndices())).toList(),
            equalTo(inResponse.getFailures().stream().flatMap(f -> Arrays.stream(f.getIndices())).toList())
        );
        final List<FieldCapabilitiesIndexResponse> inList = inResponse.getIndexResponses();
        final List<FieldCapabilitiesIndexResponse> outList = outResponse.getIndexResponses();
        assertThat(outList, hasSize(inList.size()));
        for (int i = 0; i < inList.size(); i++) {
            assertThat("Responses between old clusters don't have mapping hash", outList.get(i).getIndexMappingHash(), nullValue());
            assertThat(outList.get(i).getIndexName(), equalTo(inList.get(i).getIndexName()));
            assertThat(outList.get(i).canMatch(), equalTo(inList.get(i).canMatch()));
            Map<String, IndexFieldCapabilities> outCap = outList.get(i).get();
            Map<String, IndexFieldCapabilities> inCap = inList.get(i).get();
            if (version.onOrAfter(TransportVersion.V_8_0_0)) {
                assertThat(outCap, equalTo(inCap));
            } else {
                // Exclude metric types which was introduced in 8.0
                assertThat(outCap.keySet(), equalTo(inCap.keySet()));
                for (String field : outCap.keySet()) {
                    assertThat(outCap.get(field).getName(), equalTo(inCap.get(field).getName()));
                    assertThat(outCap.get(field).getType(), equalTo(inCap.get(field).getType()));
                    assertThat(outCap.get(field).isSearchable(), equalTo(inCap.get(field).isSearchable()));
                    assertThat(outCap.get(field).isAggregatable(), equalTo(inCap.get(field).isAggregatable()));
                    assertThat(outCap.get(field).meta(), equalTo(inCap.get(field).meta()));
                }
            }
        }
    }

    public void testReadCCSResponseFromPre82() throws Exception {
        final Version minCompactVersion = Version.CURRENT.minimumCompatibilityVersion();
        assertTrue("Remove this test once minCompactVersion >= 8.2.0", minCompactVersion.before(Version.V_8_2_0));
        String base64 = "AAADCGluZGV4XzAxAgpibHVlX2ZpZWxkCmJsdWVfZmllbGQEbG9uZwABAQAAAAlyZWRfZmllbGQJcmVkX2ZpZWxkBHRleHQAAQAAAAABC"
            + "GluZGV4XzAyAAAIaW5kZXhfMDMCDHllbGxvd19maWVsZAx5ZWxsb3dfZmllbGQHa2V5d29yZAABAQAAAAdfc2VxX25vB19zZXFfbm8EbG9uZwEBAQAAAA"
            + "EAAAAAAAAAAAA=";
        StreamInput in = StreamInput.wrap(Base64.getDecoder().decode(base64));
        in.setTransportVersion(TransportVersion.V_8_1_0);
        FieldCapabilitiesResponse nodeResp = new FieldCapabilitiesResponse(in);
        assertThat(nodeResp.getFailures(), empty());
        assertThat(
            nodeResp.getIndexResponses(),
            contains(
                new FieldCapabilitiesIndexResponse(
                    "index_01",
                    null,
                    Map.of(
                        "red_field",
                        new IndexFieldCapabilities("red_field", "text", false, true, false, false, null, Map.of()),
                        "blue_field",
                        new IndexFieldCapabilities("blue_field", "long", false, true, true, false, null, Map.of())
                    ),
                    true
                ),
                new FieldCapabilitiesIndexResponse("index_02", null, Map.of(), false),
                new FieldCapabilitiesIndexResponse(
                    "index_03",
                    null,
                    Map.of(
                        "yellow_field",
                        new IndexFieldCapabilities("yellow_field", "keyword", false, true, true, false, null, Map.of()),
                        "_seq_no",
                        new IndexFieldCapabilities("_seq_no", "long", true, true, true, false, null, Map.of())
                    ),
                    true
                )
            )
        );
    }
}
