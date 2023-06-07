/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
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
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

public class FieldCapabilitiesNodeResponseTests extends AbstractWireSerializingTestCase<FieldCapabilitiesNodeResponse> {

    @Override
    protected FieldCapabilitiesNodeResponse createTestInstance() {
        List<FieldCapabilitiesIndexResponse> responses = new ArrayList<>();
        int numResponse = randomIntBetween(0, 10);
        for (int i = 0; i < numResponse; i++) {
            responses.add(new FieldCapabilitiesIndexResponse("index_" + i, null, randomFieldCaps(), randomBoolean()));
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
            case 0 -> newResponses.add(new FieldCapabilitiesIndexResponse("extra_index", null, randomFieldCaps(), randomBoolean()));
            case 1 -> {
                int toRemove = randomInt(newResponses.size() - 1);
                newResponses.remove(toRemove);
            }
            case 2 -> {
                int toReplace = randomInt(newResponses.size() - 1);
                newResponses.set(toReplace, new FieldCapabilitiesIndexResponse("new_index", null, randomFieldCaps(), randomBoolean()));
            }
            case 3 -> {
                int toReplace = randomInt(newResponses.size() - 1);
                FieldCapabilitiesIndexResponse resp = newResponses.get(toReplace);
                newResponses.set(
                    toReplace,
                    new FieldCapabilitiesIndexResponse(resp.getIndexName(), UUIDs.randomBase64UUID(), resp.get(), true)
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
            TransportVersion.V_8_2_0,
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

    public void testSerializeNodeResponseBetweenOldNodes() throws IOException {
        final TransportVersion minCompactVersion = TransportVersion.MINIMUM_COMPATIBLE;
        assertTrue("Remove this test once minCompactVersion >= 8.2.0", minCompactVersion.before(TransportVersion.V_8_2_0));
        List<FieldCapabilitiesIndexResponse> indexResponses = CollectionUtils.concatLists(
            randomIndexResponsesWithMappingHash(randomMappingHashToIndices()),
            randomIndexResponsesWithoutMappingHash()
        );
        Randomness.shuffle(indexResponses);
        FieldCapabilitiesNodeResponse inResponse = randomNodeResponse(indexResponses);
        TransportVersion version = TransportVersionUtils.randomVersionBetween(
            random(),
            minCompactVersion,
            TransportVersionUtils.getPreviousVersion(TransportVersion.V_8_2_0)
        );
        final FieldCapabilitiesNodeResponse outResponse = copyInstance(inResponse, version);
        assertThat(outResponse.getFailures().keySet(), equalTo(inResponse.getFailures().keySet()));
        assertThat(outResponse.getUnmatchedShardIds(), equalTo(inResponse.getUnmatchedShardIds()));
        final List<FieldCapabilitiesIndexResponse> inList = inResponse.getIndexResponses();
        final List<FieldCapabilitiesIndexResponse> outList = outResponse.getIndexResponses();
        assertThat(outList, hasSize(inList.size()));
        for (int i = 0; i < inList.size(); i++) {
            assertThat("Responses between old nodes don't have mapping hash", outList.get(i).getIndexMappingHash(), nullValue());
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

    public void testReadNodeResponseFromPre82() throws Exception {
        final Version minCompactVersion = Version.CURRENT.minimumCompatibilityVersion();
        assertTrue("Remove this test once minCompactVersion >= 8.2.0", minCompactVersion.before(Version.V_8_2_0));
        String base64 = "AwhpbmRleF8wMQIKYmx1ZV9maWVsZApibHVlX2ZpZWxkBGxvbmcAAQEAAAAJcmVkX2ZpZWxkCXJlZF9maWVsZAR0ZXh0AAEAAAAAAQhpbm"
            + "RleF8wMgAACGluZGV4XzAzAgdfc2VxX25vB19zZXFfbm8EbG9uZwEBAQAAAAx5ZWxsb3dfZmllbGQMeWVsbG93X2ZpZWxkB2tleXdvcmQAAQEAAAABAAEI"
            + "aW5kZXhfMTAGdXVpZF9hAQ==";
        StreamInput in = StreamInput.wrap(Base64.getDecoder().decode(base64));
        in.setTransportVersion(TransportVersion.V_8_1_0);
        FieldCapabilitiesNodeResponse nodeResp = new FieldCapabilitiesNodeResponse(in);
        assertThat(nodeResp.getUnmatchedShardIds(), equalTo(Set.of(new ShardId("index_10", "uuid_a", 1))));
        assertThat(nodeResp.getFailures(), anEmptyMap());
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
