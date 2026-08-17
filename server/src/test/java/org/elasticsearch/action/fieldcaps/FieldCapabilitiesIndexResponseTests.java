/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class FieldCapabilitiesIndexResponseTests extends ESTestCase {

    /**
     * Ungrouped (no mapping hash) responses go through {@link FieldCapabilitiesIndexResponse#writeTo} /
     * {@link FieldCapabilitiesIndexResponse#FieldCapabilitiesIndexResponse(StreamInput)}. Verify that
     * {@code numberOfShards} survives that path.
     */
    public void testPlainSerializationRoundTripPreservesShardCount() throws IOException {
        List<FieldCapabilitiesIndexResponse> responses = new ArrayList<>();
        for (int i = 0; i < between(1, 10); i++) {
            responses.add(
                new FieldCapabilitiesIndexResponse(
                    "index_" + i,
                    null,
                    randomFieldCaps(),
                    randomBoolean(),
                    randomFrom(IndexMode.availableModes()),
                    between(1, 100)
                )
            );
        }
        BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(TransportVersion.current());
        FieldCapabilitiesIndexResponse.writeList(out, responses);

        StreamInput in = out.bytes().streamInput();
        in.setTransportVersion(TransportVersion.current());
        List<FieldCapabilitiesIndexResponse> result = FieldCapabilitiesIndexResponse.readList(in);

        assertThat(result.size(), equalTo(responses.size()));
        Map<String, Integer> expected = responses.stream()
            .collect(Collectors.toMap(FieldCapabilitiesIndexResponse::getIndexName, FieldCapabilitiesIndexResponse::getNumberOfShards));
        for (FieldCapabilitiesIndexResponse r : result) {
            assertThat("index " + r.getIndexName(), r.getNumberOfShards(), equalTo(expected.get(r.getIndexName())));
        }
    }

    /**
     * Two indices sharing a mapping hash (compressed codec path) may have different shard counts.
     * Verify the per-index count is preserved — not collapsed to a single value for the group.
     */
    public void testCompressedSerializationPreservesPerIndexShardCount() throws IOException {
        String mappingHash = randomIdentifier();
        Map<String, IndexFieldCapabilities> sharedFieldCaps = randomFieldCaps();
        IndexMode indexMode = randomFrom(IndexMode.availableModes());
        int shards1 = between(1, 10);
        int shards2 = shards1 + between(1, 10); // guaranteed different
        List<FieldCapabilitiesIndexResponse> responses = List.of(
            new FieldCapabilitiesIndexResponse("index_a", mappingHash, sharedFieldCaps, true, indexMode, shards1),
            new FieldCapabilitiesIndexResponse("index_b", mappingHash, sharedFieldCaps, true, indexMode, shards2)
        );

        BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(TransportVersion.current());
        FieldCapabilitiesIndexResponse.writeList(out, responses);

        StreamInput in = out.bytes().streamInput();
        in.setTransportVersion(TransportVersion.current());
        List<FieldCapabilitiesIndexResponse> result = FieldCapabilitiesIndexResponse.readList(in);

        assertThat(result.size(), equalTo(2));
        Map<String, Integer> actual = result.stream()
            .collect(Collectors.toMap(FieldCapabilitiesIndexResponse::getIndexName, FieldCapabilitiesIndexResponse::getNumberOfShards));
        assertThat(actual.get("index_a"), equalTo(shards1));
        assertThat(actual.get("index_b"), equalTo(shards2));
    }

    /**
     * Data written by nodes that pre-date shard-count propagation must deserialize to
     * {@code numberOfShards == 0} (unknown) on current nodes.
     */
    public void testOldVersionDefaultsShardCountToZero() throws IOException {
        List<FieldCapabilitiesIndexResponse> responses = List.of(
            new FieldCapabilitiesIndexResponse("idx", null, randomFieldCaps(), true, IndexMode.STANDARD, between(1, 100))
        );

        // Simulate an old node: write without shard counts.
        BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(TransportVersion.minimumCompatible());
        FieldCapabilitiesIndexResponse.writeList(out, responses);

        // Read as if we received bytes from that old node (transport version set to old version).
        StreamInput in = out.bytes().streamInput();
        in.setTransportVersion(TransportVersion.minimumCompatible());
        List<FieldCapabilitiesIndexResponse> result = FieldCapabilitiesIndexResponse.readList(in);

        assertThat(result.size(), equalTo(1));
        assertThat(result.get(0).getNumberOfShards(), equalTo(0));
    }

    static Map<String, IndexFieldCapabilities> randomFieldCaps() {
        final Map<String, IndexFieldCapabilities> fieldCaps = new HashMap<>();
        final Map<String, String> meta = switch (randomInt(2)) {
            case 0 -> Map.of();
            case 1 -> Map.of("key", "value");
            default -> Map.of("key1", "value1", "key2", "value2");
        };
        final TimeSeriesParams.MetricType metricType = randomBoolean() ? null : randomFrom(TimeSeriesParams.MetricType.values());
        final List<String> fields = randomList(1, 5, () -> randomAlphaOfLength(5));
        for (String field : fields) {
            final IndexFieldCapabilities fieldCap = new IndexFieldCapabilities(
                field,
                randomAlphaOfLengthBetween(5, 20),
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                metricType,
                meta
            );
            fieldCaps.put(field, fieldCap);
        }
        return fieldCaps;
    }

    static Map<String, List<String>> randomMappingHashToIndices() {
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

    static List<FieldCapabilitiesIndexResponse> randomIndexResponsesWithMappingHash(Map<String, List<String>> mappingHashToIndices) {
        final List<FieldCapabilitiesIndexResponse> responses = new ArrayList<>();
        for (Map.Entry<String, List<String>> e : mappingHashToIndices.entrySet()) {
            Map<String, IndexFieldCapabilities> fieldCaps = randomFieldCaps();
            var indexMode = randomFrom(IndexMode.availableModes());
            String mappingHash = e.getKey();
            for (String index : e.getValue()) {
                responses.add(new FieldCapabilitiesIndexResponse(index, mappingHash, fieldCaps, true, indexMode));
            }
        }
        return responses;
    }

    static List<FieldCapabilitiesIndexResponse> randomIndexResponsesWithoutMappingHash() {
        final List<FieldCapabilitiesIndexResponse> responses = new ArrayList<>();
        int numIndices = between(0, 10);
        for (int i = 0; i < numIndices; i++) {
            String index = "index_without_mapping_hash_" + i;
            var indexMode = randomFrom(IndexMode.availableModes());
            responses.add(new FieldCapabilitiesIndexResponse(index, null, randomFieldCaps(), randomBoolean(), indexMode));
        }
        return responses;
    }
}
