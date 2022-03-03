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
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

public class FieldCapabilitiesIndexResponseTests extends ESTestCase {

    public void testShareResponsesUsingMappingHash() throws Exception {
        final Supplier<Map<String, IndexFieldCapabilities>> randomFieldCaps = () -> {
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
        };
        final List<FieldCapabilitiesIndexResponse> inList = new ArrayList<>();
        int numGroups = randomIntBetween(0, 20);
        Map<String, List<String>> mappingHashToIndices = new HashMap<>();
        for (int i = 0; i < numGroups; i++) {
            String groupName = "group_" + i;
            String hashing = UUIDs.randomBase64UUID();
            List<String> indices = IntStream.range(0, randomIntBetween(1, 5)).mapToObj(n -> groupName + "_" + n).toList();
            mappingHashToIndices.put(hashing, indices);
            Map<String, IndexFieldCapabilities> fieldCaps = randomFieldCaps.get();
            for (String index : indices) {
                inList.add(new FieldCapabilitiesIndexResponse(index, hashing, fieldCaps, true));
            }
        }
        int numUngroups = randomIntBetween(0, 5);
        for (int i = 0; i < numUngroups; i++) {
            String index = "ungrouped_" + i;
            final String hashing;
            final boolean canMatch;
            Map<String, IndexFieldCapabilities> fieldCaps = Map.of();
            if (randomBoolean()) {
                canMatch = false;
                hashing = UUIDs.randomBase64UUID();
            } else {
                canMatch = randomBoolean();
                hashing = null;
                if (canMatch) {
                    fieldCaps = randomFieldCaps.get();
                }
            }
            inList.add(new FieldCapabilitiesIndexResponse(index, hashing, fieldCaps, canMatch));
        }
        Randomness.shuffle(inList);
        // old versions
        {
            final Version oldVersion = VersionUtils.randomPreviousCompatibleVersion(random(), Version.V_8_2_0);
            final List<FieldCapabilitiesIndexResponse> serializedList;
            try (BytesStreamOutput output = new BytesStreamOutput()) {
                output.setVersion(oldVersion);
                FieldCapabilitiesIndexResponse.writeList(output, inList);
                try (
                    StreamInput in = new NamedWriteableAwareStreamInput(
                        output.bytes().streamInput(),
                        new NamedWriteableRegistry(Collections.emptyList())
                    )
                ) {
                    in.setVersion(oldVersion);
                    serializedList = FieldCapabilitiesIndexResponse.readList(in);
                }
            }
            assertThat(serializedList, hasSize(inList.size()));
            for (int i = 0; i < inList.size(); i++) {
                FieldCapabilitiesIndexResponse serialized = serializedList.get(i);
                assertThat(serialized.getIndexMappingHash(), nullValue());
                FieldCapabilitiesIndexResponse in = inList.get(i);
                assertThat(serialized.get(), equalTo(in.get()));
                assertThat(serialized.getIndexName(), equalTo(in.getIndexName()));
                assertThat(serialized.canMatch(), equalTo(in.canMatch()));
            }
        }
        // new versions
        {
            final List<FieldCapabilitiesIndexResponse> serializedList;
            final Version newVersion = VersionUtils.randomVersionBetween(random(), Version.V_8_2_0, Version.CURRENT);
            try (BytesStreamOutput output = new BytesStreamOutput()) {
                output.setVersion(newVersion);
                FieldCapabilitiesIndexResponse.writeList(output, inList);
                try (
                    StreamInput in = new NamedWriteableAwareStreamInput(
                        output.bytes().streamInput(),
                        new NamedWriteableRegistry(Collections.emptyList())
                    )
                ) {
                    in.setVersion(newVersion);
                    serializedList = FieldCapabilitiesIndexResponse.readList(in);
                }
            }
            assertThat(
                serializedList.stream().sorted(Comparator.comparing(FieldCapabilitiesIndexResponse::getIndexName)).toList(),
                equalTo(inList.stream().sorted(Comparator.comparing(FieldCapabilitiesIndexResponse::getIndexName)).toList())
            );
            Map<String, List<FieldCapabilitiesIndexResponse>> groupedResponses = serializedList.stream()
                .filter(r -> r.canMatch() && r.getIndexMappingHash() != null)
                .collect(Collectors.groupingBy(FieldCapabilitiesIndexResponse::getIndexMappingHash));
            assertThat(groupedResponses.keySet(), equalTo(mappingHashToIndices.keySet()));
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

    }
}
