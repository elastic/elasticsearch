/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FieldCapabilitiesNodeResponseTests extends AbstractWireSerializingTestCase<FieldCapabilitiesNodeResponse> {

    @Override
    protected FieldCapabilitiesNodeResponse createTestInstance() {
        MergeResultsMode mergeMode = randomFrom(MergeResultsMode.NO_MERGE, MergeResultsMode.INTERNAL_PARTIAL_MERGE);
        Set<ShardId> unmatchedShardIds = new HashSet<>();
        int numIndices = randomIntBetween(0, 10);
        if (mergeMode == MergeResultsMode.NO_MERGE) {
            List<FieldCapabilitiesIndexResponse> responses = new ArrayList<>();
            for (int i = 0; i < numIndices; i++) {
                responses.add(FieldCapTestHelper.createRandomIndexResponse());
            }
            return new FieldCapabilitiesNodeResponse(responses, Collections.emptyList(), unmatchedShardIds);
        } else {
            int numResponse = randomIntBetween(0, 10);
            List<String> indices = new ArrayList<>();
            for (int i = 0; i < numResponse; i++) {
                indices.add(randomAlphaOfLength(10));
            }
            Map<String, Map<String, FieldCapabilities.Builder>> responseMap = new HashMap<>();
            if (indices.isEmpty() == false) {
                int numFields = randomIntBetween(1, 10);
                for (int i = 0; i < numFields; i++) {
                    String field = "field-" + i;
                    Map<String, FieldCapabilities.Builder> fieldMap = responseMap.computeIfAbsent(field, f -> new HashMap<>());
                    int numTypes = randomIntBetween(1, 10);
                    for (int j = 0; j < numTypes; j++) {
                        FieldCapabilities.Builder builder = fieldMap.computeIfAbsent("type-" + j, t -> new FieldCapabilities.Builder());
                        final List<String> indicesWithType = randomSubsetOf(between(1, indices.size()), indices);
                        for (String index : indicesWithType) {
                            Map<String, String> meta = new HashMap<>();
                            if (randomBoolean()) {
                                meta.put("field1", "value1");
                            }
                            if (randomBoolean()) {
                                meta.put("field2", "value2");
                            }
                            builder.add(index, randomBoolean(), randomBoolean(), randomBoolean(), meta);
                        }
                    }
                }
            }
            return new FieldCapabilitiesNodeResponse(indices, responseMap, Collections.emptyList(), unmatchedShardIds);
        }
    }

    @Override
    protected Writeable.Reader<FieldCapabilitiesNodeResponse> instanceReader() {
        return FieldCapabilitiesNodeResponse::new;
    }

    @Override
    protected FieldCapabilitiesNodeResponse mutateInstance(FieldCapabilitiesNodeResponse response) {
        List<FieldCapabilitiesIndexResponse> newResponses = new ArrayList<>(response.getIndexResponses());
        int mutation = response.getIndexResponses().isEmpty() ? 0 : randomIntBetween(0, 2);
        switch (mutation) {
            case 0:
                newResponses.add(FieldCapTestHelper.createRandomIndexResponse());
                break;
            case 1:
                int toRemove = randomInt(newResponses.size() - 1);
                newResponses.remove(toRemove);
                break;
            case 2:
                int toReplace = randomInt(newResponses.size() - 1);
                newResponses.set(toReplace, FieldCapTestHelper.createRandomIndexResponse());
                break;
        }
        return new FieldCapabilitiesNodeResponse(newResponses, Collections.emptyList(), Collections.emptySet());
    }
}
