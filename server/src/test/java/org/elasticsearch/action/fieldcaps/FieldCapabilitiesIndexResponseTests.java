/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.Version;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

public class FieldCapabilitiesIndexResponseTests extends ESTestCase {

    private static Map<String, IndexFieldCapabilities> randomFieldCaps() {
        final Map<String, IndexFieldCapabilities> fieldCaps = new HashMap<>();
        final List<String> fields = randomList(1, 5, () -> randomAlphaOfLength(5));
        for (String field : fields) {
            final IndexFieldCapabilities fieldCap = new IndexFieldCapabilities(
                field,
                randomAlphaOfLengthBetween(5, 20),
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                false,
                null,
                Map.of()
            );
            fieldCaps.put(field, fieldCap);
        }
        return fieldCaps;
    }

    private Map<String, List<String>> randomMappingHashToIndices() {
        Map<String, List<String>> mappingHashToIndices = new HashMap<>();
        int numGroups = between(0, 10);
        for (int g = 0; g < numGroups; g++) {
            String mappingHash = "mapping_hash_" + g;
            String group = "group_" + g;
            List<String> indices = IntStream.range(0, between(1, 10)).mapToObj(n -> group + "_index_" + n).toList();
            mappingHashToIndices.put(mappingHash, indices);
        }
        return mappingHashToIndices;
    }

    private List<FieldCapabilitiesIndexResponse> randomIndexResponses(Map<String, List<String>> mappingHashToIndices) {
        final List<FieldCapabilitiesIndexResponse> responses = new ArrayList<>();
        for (Map.Entry<String, List<String>> e : mappingHashToIndices.entrySet()) {
            Map<String, IndexFieldCapabilities> fieldCaps = randomFieldCaps();
            String mappingHash = e.getKey();
            for (String index : e.getValue()) {
                responses.add(new FieldCapabilitiesIndexResponse(index, mappingHash, fieldCaps, true));
            }
        }
        return responses;
    }

    private List<FieldCapabilitiesIndexResponse> randomIndexResponsesWithoutMappingHash() {
        final List<FieldCapabilitiesIndexResponse> responses = new ArrayList<>();
        int numIndices = between(0, 10);
        for (int i = 0; i < numIndices; i++) {
            String index = "index_without_mapping_hash_" + i;
            responses.add(new FieldCapabilitiesIndexResponse(index, null, randomFieldCaps(), randomBoolean()));
        }
        return responses;
    }

    public void testWriteResponsesBetweenNewNodes() throws Exception {
        Map<String, List<String>> mappingHashToIndices = randomMappingHashToIndices();
        List<FieldCapabilitiesIndexResponse> responseList = CollectionUtils.concatLists(
            randomIndexResponses(mappingHashToIndices),
            randomIndexResponsesWithoutMappingHash()
        );
        assertSerializeOnNewNodes(responseList, mappingHashToIndices);
        final Version minCompactVersion = Version.CURRENT.minimumCompatibilityVersion();
        if (minCompactVersion.before(Version.V_8_2_0)) {
            assertSerializeOnOldNodes(responseList, minCompactVersion);
        }
    }

    public void testWriteResponsesBetweenOldNodes() throws IOException {
        final Version minCompactVersion = Version.CURRENT.minimumCompatibilityVersion();
        assumeTrue("Write list with mapping hash is introduced in 8.2", minCompactVersion.before(Version.V_8_2_0));
        Map<String, List<String>> mappingHashToIndices = randomMappingHashToIndices();
        List<FieldCapabilitiesIndexResponse> responseList = CollectionUtils.concatLists(
            randomIndexResponses(mappingHashToIndices),
            randomIndexResponsesWithoutMappingHash()
        );
        assertSerializeOnOldNodes(responseList, minCompactVersion);
    }

    public void testReadResponsesFromOldNode() throws Exception {
        final Version minCompactVersion = Version.CURRENT.minimumCompatibilityVersion();
        assumeTrue("Write list with mapping hash is introduced in 8.2", minCompactVersion.before(Version.V_8_2_0));
        String base64FromES80 =
            "AwhpbmRleF8wMQIJcmVkX2ZpZWxkCXJlZF9maWVsZAR0ZXh0AAEAAAAACmJsdWVfZmllbGQJcmVkX2ZpZWxkBGxvbmcAAQEAAAABCGluZGV4XzAyAAAIaW5kZXhfMDMCB19zZXFfbm8HX3NlcV9ubwRsb25nAQEBAAAADHllbGxvd19maWVsZAx5ZWxsb3dfZmllbGQHa2V5d29yZAABAQAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAA==";
        StreamInput in = StreamInput.wrap(Base64.getDecoder().decode(base64FromES80));
        in.setVersion(VersionUtils.randomVersionBetween(random(), Version.V_8_0_0, VersionUtils.getPreviousVersion(Version.V_8_2_0)));
        List<FieldCapabilitiesIndexResponse> responses = FieldCapabilitiesIndexResponse.readList(in);
        List<FieldCapabilitiesIndexResponse> expectedResponses = List.of(
            new FieldCapabilitiesIndexResponse(
                "index_01",
                null,
                Map.of(
                    "red_field",
                    new IndexFieldCapabilities("red_field", "text", false, true, false, false, null, Map.of()),
                    "blue_field",
                    new IndexFieldCapabilities("red_field", "long", false, true, true, false, null, Map.of())
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
        );
        assertThat(responses, equalTo(expectedResponses));
        assertSerializeOnNewNodes(new ArrayList<>(responses), Map.of());
        assertSerializeOnOldNodes(new ArrayList<>(responses), minCompactVersion);
    }

    /**
     * Asserts the serialization of a list of index responses between two nodes on 8.2 or later. The serialized list must contain
     * the same elements of the input list but can be in any order. Importantly, the elements of the serialized list must be shared
     * between indices that have the same mapping hashes specified by {@code mappingHashToIndices}.
     */
    private void assertSerializeOnNewNodes(List<FieldCapabilitiesIndexResponse> inList, Map<String, List<String>> mappingHashToIndices)
        throws IOException {
        final Version[] versions = new Version[between(2, 5)];
        for (int i = 0; i < versions.length; i++) {
            versions[i] = VersionUtils.randomVersionBetween(random(), Version.V_8_2_0, Version.CURRENT);
        }
        for (Version version : versions) {
            Randomness.shuffle(inList);
            final List<FieldCapabilitiesIndexResponse> outList;
            try (BytesStreamOutput output = new BytesStreamOutput()) {
                output.setVersion(version);
                FieldCapabilitiesIndexResponse.writeList(output, inList);
                try (StreamInput in = output.bytes().streamInput()) {
                    in.setVersion(version);
                    outList = FieldCapabilitiesIndexResponse.readList(in);
                }
            }
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
            inList = new ArrayList<>(outList);
        }
    }

    /**
     * Asserts the serialization of a list of index responses between two nodes before 8.2. The serialized list must contain the same
     * elements of the input list except that the elements of the serialized list don't have mapping hash. After the first iteration,
     * this method can perform the serialization between nodes of any version.
     */
    private void assertSerializeOnOldNodes(List<FieldCapabilitiesIndexResponse> inList, Version minCompactVersion) throws IOException {
        final Version[] wireVersions = new Version[between(2, 5)];
        // old version first, then any version
        wireVersions[0] = VersionUtils.randomVersionBetween(random(), minCompactVersion, VersionUtils.getPreviousVersion(Version.V_8_2_0));
        for (int i = 1; i < wireVersions.length; i++) {
            wireVersions[i] = VersionUtils.randomVersionBetween(random(), minCompactVersion, Version.CURRENT);
        }
        for (Version wireVersion : wireVersions) {
            Randomness.shuffle(inList);
            final List<FieldCapabilitiesIndexResponse> outList;
            try (BytesStreamOutput output = new BytesStreamOutput()) {
                output.setVersion(wireVersion);
                FieldCapabilitiesIndexResponse.writeList(output, inList);
                try (StreamInput in = output.bytes().streamInput()) {
                    in.setVersion(wireVersion);
                    outList = FieldCapabilitiesIndexResponse.readList(in);
                }
            }
            assertThat(outList, hasSize(inList.size()));
            for (int i = 0; i < inList.size(); i++) {
                FieldCapabilitiesIndexResponse resp = outList.get(i);
                assertThat("Responses between old nodes don't have mapping hash", resp.getIndexMappingHash(), nullValue());
                FieldCapabilitiesIndexResponse in = inList.get(i);
                assertThat(resp.get(), equalTo(in.get()));
                assertThat(resp.getIndexName(), equalTo(in.getIndexName()));
                assertThat(resp.canMatch(), equalTo(in.canMatch()));
            }
            inList = new ArrayList<>(outList);
        }
    }
}
