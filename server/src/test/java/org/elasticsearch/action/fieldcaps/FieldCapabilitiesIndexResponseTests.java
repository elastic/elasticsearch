/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class FieldCapabilitiesIndexResponseTests extends ESTestCase {

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
            String mappingHash = e.getKey();
            for (String index : e.getValue()) {
                responses.add(new FieldCapabilitiesIndexResponse(index, mappingHash, fieldCaps, true));
            }
        }
        return responses;
    }

    static List<FieldCapabilitiesIndexResponse> randomIndexResponsesWithoutMappingHash() {
        final List<FieldCapabilitiesIndexResponse> responses = new ArrayList<>();
        int numIndices = between(0, 10);
        for (int i = 0; i < numIndices; i++) {
            String index = "index_without_mapping_hash_" + i;
            responses.add(new FieldCapabilitiesIndexResponse(index, null, randomFieldCaps(), randomBoolean()));
        }
        return responses;
    }
}
