/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;

/**
 * A cast of a field mapped to several types is lenient on the types it doesn't convert, for every pair of mapped types and every
 * target type.
 * <p>
 * The oracle is the cast of the same field in an index of its own: the union, a field that has a conversion for each type it's mapped
 * as, must give for each index what the cast gives when that index is the only one. A type ES|QL doesn't support is {@code null}.
 * </p>
 */
public class LenientUnionCastIT extends AbstractEsqlIntegTestCase {

    /** A mapping type, and a value of it that is in the index of that type. */
    private record MappedType(String name, Object value) {
        String index() {
            return "lenient_union_" + name;
        }
    }

    private static final List<MappedType> TYPES = List.of(
        new MappedType("keyword", "7"),
        new MappedType("text", "7"),
        new MappedType("long", 7L),
        new MappedType("integer", 7),
        new MappedType("short", 7),
        new MappedType("float", 7.5),
        new MappedType("boolean", true),
        new MappedType("date", "2024-01-02T03:04:05.678Z"),
        new MappedType("date_nanos", "2024-01-02T03:04:05.678901234Z"),
        new MappedType("ip", "10.0.0.1"),
        new MappedType("geo_point", "POINT (1 2)"),
        // A type ES|QL doesn't support
        new MappedType("binary", "aGk=")
    );

    private static final List<String> TARGETS = List.of("keyword", "long", "double", "boolean", "ip", "datetime");

    private static boolean isSupported(MappedType type) {
        return type.name().equals("binary") == false;
    }

    public void testEveryPairOfTypesAndEveryTarget() {
        for (MappedType type : TYPES) {
            assertAcked(
                client().admin()
                    .indices()
                    .prepareCreate(type.index())
                    .setSettings(Settings.builder().put("index.number_of_shards", 1))
                    .setMapping("v", "type=" + type.name())
                    .execute()
                    .actionGet(TimeValue.timeValueMinutes(2))
            );
        }
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (MappedType type : TYPES) {
            bulk.add(new IndexRequest(type.index()).source("v", type.value()));
        }
        BulkResponse bulkResponse = bulk.get();
        assertFalse(bulkResponse.buildFailureMessage(), bulkResponse.hasFailures());

        Map<String, List<Object>> expectedByIndex = new HashMap<>();
        for (MappedType type : TYPES) {
            List<Object> expected;
            if (isSupported(type)) {
                try (EsqlQueryResponse response = run("FROM " + type.index() + " | EVAL " + casts() + " | KEEP " + targetNames())) {
                    expected = getValuesList(response).getFirst();
                }
            } else {
                expected = new ArrayList<>();
                TARGETS.forEach(t -> expected.add(null));
            }
            expectedByIndex.put(type.index(), expected);
        }

        for (int i = 0; i < TYPES.size(); i++) {
            for (int j = i + 1; j < TYPES.size(); j++) {
                MappedType first = TYPES.get(i);
                MappedType second = TYPES.get(j);
                String query = "FROM "
                    + first.index()
                    + ","
                    + second.index()
                    + " METADATA _index | EVAL "
                    + casts()
                    + " | SORT _index | KEEP _index, "
                    + targetNames();
                try (EsqlQueryResponse response = run(query)) {
                    List<List<Object>> actual = getValuesList(response);
                    assertThat(query, actual.size(), equalTo(2));
                    for (List<Object> row : actual) {
                        String index = (String) row.getFirst();
                        assertThat(query + ", index [" + index + "]", row.subList(1, row.size()), equalTo(expectedByIndex.get(index)));
                    }
                }
            }
        }
    }

    /** {@code v} cast to every target, as the fields named like the target. */
    private static String casts() {
        return TARGETS.stream().map(target -> "to_" + target + " = v::" + target).collect(Collectors.joining(", "));
    }

    private static String targetNames() {
        return TARGETS.stream().map(target -> "to_" + target).collect(Collectors.joining(", "));
    }
}
