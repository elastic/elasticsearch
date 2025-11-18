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
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponseTests.randomFieldCaps;
import static org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponseTests.randomIndexResponsesWithMappingHash;
import static org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponseTests.randomIndexResponsesWithoutMappingHash;
import static org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponseTests.randomMappingHashToIndices;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class FieldCapabilitiesNodeResponseTests extends AbstractWireSerializingTestCase<FieldCapabilitiesNodeResponse> {

    @Override
    protected FieldCapabilitiesNodeResponse createTestInstance() {
        List<FieldCapabilitiesIndexResponse> responses = new ArrayList<>();
        int numResponse = randomIntBetween(0, 10);
        for (int i = 0; i < numResponse; i++) {
            responses.add(
                new FieldCapabilitiesIndexResponse("index_" + i, null, randomFieldCaps(), randomBoolean(), randomFrom(IndexMode.values()))
            );
        }
        int numUnmatched = randomIntBetween(0, 3);
        Set<ShardId> shardIds = new HashSet<>();
        for (int i = 0; i < numUnmatched; i++) {
            shardIds.add(new ShardId(randomAlphaOfLength(10), randomAlphaOfLength(10), between(0, 10)));
        }
        return new FieldCapabilitiesNodeResponse(responses, Collections.emptyMap(), shardIds);
    }

    @Override
    protected Writeable.Reader<FieldCapabilitiesNodeResponse> instanceReader() {
        return FieldCapabilitiesNodeResponse::new;
    }

    @Override
    protected FieldCapabilitiesNodeResponse mutateInstance(FieldCapabilitiesNodeResponse response) {
        List<FieldCapabilitiesIndexResponse> newResponses = new ArrayList<>(response.getIndexResponses());
        int mutation = response.getIndexResponses().isEmpty() ? 0 : randomIntBetween(0, 3);
        switch (mutation) {
            case 0 -> newResponses.add(
                new FieldCapabilitiesIndexResponse("extra_index", null, randomFieldCaps(), randomBoolean(), randomFrom(IndexMode.values()))
            );
            case 1 -> {
                int toRemove = randomInt(newResponses.size() - 1);
                newResponses.remove(toRemove);
            }
            case 2 -> {
                int toReplace = randomInt(newResponses.size() - 1);
                newResponses.set(
                    toReplace,
                    new FieldCapabilitiesIndexResponse(
                        "new_index",
                        null,
                        randomFieldCaps(),
                        randomBoolean(),
                        randomFrom(IndexMode.values())
                    )
                );
            }
            case 3 -> {
                int toReplace = randomInt(newResponses.size() - 1);
                FieldCapabilitiesIndexResponse resp = newResponses.get(toReplace);
                newResponses.set(
                    toReplace,
                    new FieldCapabilitiesIndexResponse(
                        resp.getIndexName(),
                        UUIDs.randomBase64UUID(),
                        resp.get(),
                        true,
                        randomFrom(IndexMode.values())
                    )
                );
            }
        }
        return new FieldCapabilitiesNodeResponse(newResponses, Collections.emptyMap(), response.getUnmatchedShardIds());
    }

    public void testSerializeNodeResponseBetweenNewNodes() throws Exception {
        Map<String, List<String>> mappingHashToIndices = randomMappingHashToIndices();
        List<FieldCapabilitiesIndexResponse> indexResponses = CollectionUtils.concatLists(
            randomIndexResponsesWithMappingHash(mappingHashToIndices),
            randomIndexResponsesWithoutMappingHash()
        );
        Randomness.shuffle(indexResponses);
        FieldCapabilitiesNodeResponse inNode = randomNodeResponse(indexResponses);
        final TransportVersion version = TransportVersionUtils.randomVersionBetween(
            random(),
            TransportVersions.V_8_2_0,
            TransportVersion.current()
        );
        final FieldCapabilitiesNodeResponse outNode = copyInstance(inNode, version);
        assertThat(outNode.getFailures().keySet(), equalTo(inNode.getFailures().keySet()));
        assertThat(outNode.getUnmatchedShardIds(), equalTo(inNode.getUnmatchedShardIds()));
        final List<FieldCapabilitiesIndexResponse> inList = inNode.getIndexResponses();
        final List<FieldCapabilitiesIndexResponse> outList = outNode.getIndexResponses();
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

    private static FieldCapabilitiesNodeResponse randomNodeResponse(List<FieldCapabilitiesIndexResponse> indexResponses) {
        int numUnmatched = randomIntBetween(0, 3);
        final Set<ShardId> unmatchedShardIds = new HashSet<>();
        for (int i = 0; i < numUnmatched; i++) {
            unmatchedShardIds.add(new ShardId(randomAlphaOfLength(10), randomAlphaOfLength(10), between(0, 10)));
        }
        final int failedShards = randomIntBetween(0, 3);
        final Map<ShardId, Exception> failures = new HashMap<>();
        for (int i = 0; i < failedShards; i++) {
            ShardId shardId = new ShardId(randomAlphaOfLength(10), randomAlphaOfLength(10), between(0, 10));
            failures.put(shardId, new IllegalStateException(randomAlphaOfLength(10)));
        }
        return new FieldCapabilitiesNodeResponse(indexResponses, failures, unmatchedShardIds);
    }
}
