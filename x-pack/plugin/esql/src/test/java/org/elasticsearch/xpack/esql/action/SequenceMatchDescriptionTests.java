/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

/** Exercises malformed consumer inputs separately from the server's final-schema construction. */
public class SequenceMatchDescriptionTests extends ESTestCase {
    public void testIdentityComesFromMemberReferencesAndNumbersRemainLossless() {
        var description = SequenceMatchDescription.fromColumns(columns(contract()));
        long exact = 9_007_199_254_740_993L;
        Map<String, Object> source = Map.of("long", exact, "nested", Map.of("values", Arrays.asList("x", null, "x")));
        var row = description.decodeRow(
            List.of("forged-primary-id", exact, "same-index", "same-id", source, "same-index", "same-id", source),
            SequenceMatchDescription.SourcePolicy.REQUIRE_PRESENT
        );
        assertEquals(2, description.primaryStage());
        assertEquals(List.of("_id"), description.ordinaryNames());
        assertEquals(List.of("forged-primary-id"), row.ordinary());
        assertEquals(List.of(exact), row.keys());
        assertEquals("same-id", description.primary(row).id());
        assertEquals(source, description.primary(row).source());
        assertThat(description.primary(row).source().get("long"), instanceOf(Long.class));
        // Equal published labels do not collapse the two separately ordered member occurrences.
        assertEquals(List.of(1, 2), row.members().stream().map(SequenceMatchDescription.Member::stage).toList());
    }

    public void testEmptyAndKeylessSchemaStillRequiresAndDecodesDescriptor() {
        Map<String, Object> contract = contract();
        contract.put("keys", List.of());
        var description = SequenceMatchDescription.fromColumns(columns(contract));
        assertEquals(List.of("_id", "same-display-name"), description.ordinaryNames());
        assertEquals(2, description.members().size());
    }

    public void testSourceAvailabilityIsAnExplicitPolicy() {
        var description = SequenceMatchDescription.fromColumns(columns(contract()));
        List<Object> values = Arrays.asList("edited", 1L, "a", "A", null, "b", "B", Map.of());
        assertNull(description.decodeRow(values, SequenceMatchDescription.SourcePolicy.ALLOW_NULL).members().getFirst().source());
        var failure = expectThrows(
            IllegalArgumentException.class,
            () -> description.decodeRow(values, SequenceMatchDescription.SourcePolicy.REQUIRE_PRESENT)
        );
        assertThat(failure.getMessage(), containsString("source is unavailable"));
        expectThrows(IllegalArgumentException.class, () -> description.decodeRow(values, null));
    }

    public void testRejectsMissingUnknownAndMultipleDescriptors() {
        for (String field : List.of("version", "layout")) {
            Map<String, Object> contract = contract();
            contract.put(field, field.equals("version") ? 2 : "grouped");
            expectThrows(IllegalArgumentException.class, () -> SequenceMatchDescription.fromColumns(columns(contract)));
        }
        List<ColumnInfo> missing = columns(contract());
        missing.set(2, column("same-display-name", DataType.KEYWORD, "c2", null));
        expectThrows(IllegalArgumentException.class, () -> SequenceMatchDescription.fromColumns(missing));
        List<ColumnInfo> multiple = columns(contract());
        multiple.set(0, column("_id", DataType.KEYWORD, "c0", contract()));
        expectThrows(IllegalArgumentException.class, () -> SequenceMatchDescription.fromColumns(multiple));
    }

    public void testRejectsMissingAndDuplicateColumnIdsWithoutFallingBackToNames() {
        List<ColumnInfo> missing = columns(contract());
        missing.set(0, new ColumnInfoImpl("_id", DataType.KEYWORD, null));
        expectThrows(IllegalArgumentException.class, () -> SequenceMatchDescription.fromColumns(missing));
        List<ColumnInfo> duplicate = columns(contract());
        duplicate.set(0, column("_id", DataType.KEYWORD, "c3", null));
        expectThrows(IllegalArgumentException.class, () -> SequenceMatchDescription.fromColumns(duplicate));
        List<ColumnInfo> blank = columns(contract());
        blank.set(0, column("_id", DataType.KEYWORD, "", null));
        expectThrows(IllegalArgumentException.class, () -> SequenceMatchDescription.fromColumns(blank));
    }

    public void testRejectsWrongMissingAndSharedRoleReferences() {
        for (String id : List.of("unknown", "c0", "c2")) {
            Map<String, Object> first = member(1, "c2", id, "c4");
            Map<String, Object> contract = contract();
            contract.put("members", List.of(first, member(2, "c5", "c6", "c7")));
            if (id.equals("c0")) {
                List<ColumnInfo> columns = columns(contract);
                columns.set(0, column("_id", DataType.LONG, "c0", null));
                expectThrows(IllegalArgumentException.class, () -> SequenceMatchDescription.fromColumns(columns));
            } else {
                expectThrows(IllegalArgumentException.class, () -> SequenceMatchDescription.fromColumns(columns(contract)));
            }
        }
        List<ColumnInfo> wrongSourceType = columns(contract());
        wrongSourceType.set(4, column("same-display-name", DataType.KEYWORD, "c4", null));
        expectThrows(IllegalArgumentException.class, () -> SequenceMatchDescription.fromColumns(wrongSourceType));
    }

    public void testRejectsUnorderedMembersInvalidPrimaryAndCount() {
        Map<String, Object> reversed = contract();
        reversed.put("members", List.of(member(2, "c5", "c6", "c7"), member(1, "c2", "c3", "c4")));
        expectThrows(IllegalArgumentException.class, () -> SequenceMatchDescription.fromColumns(columns(reversed)));
        for (int primary : new int[] { 0, 3 }) {
            Map<String, Object> invalid = contract();
            invalid.put("primary_stage", primary);
            expectThrows(IllegalArgumentException.class, () -> SequenceMatchDescription.fromColumns(columns(invalid)));
        }
        Map<String, Object> count = contract();
        count.put("member_count", 3);
        expectThrows(IllegalArgumentException.class, () -> SequenceMatchDescription.fromColumns(columns(count)));
        Map<String, Object> single = contract();
        single.put("members", List.of(member(1, "c2", "c3", "c4")));
        single.put("member_count", 1);
        single.put("primary_stage", 1);
        expectThrows(IllegalArgumentException.class, () -> SequenceMatchDescription.fromColumns(columns(single)));
    }

    public void testRejectsMovedAnchorAndMalformedDescriptorValues() {
        List<ColumnInfo> moved = columns(contract());
        moved.set(2, column("same-display-name", DataType.KEYWORD, "c2", null));
        moved.set(3, column("same-display-name", DataType.KEYWORD, "c3", contract()));
        expectThrows(IllegalArgumentException.class, () -> SequenceMatchDescription.fromColumns(moved));
        Map<String, Object> fractional = contract();
        fractional.put("primary_stage", 1.5);
        expectThrows(IllegalArgumentException.class, () -> SequenceMatchDescription.fromColumns(columns(fractional)));
        Map<String, Object> wrongMembers = contract();
        wrongMembers.put("members", "not-an-array");
        expectThrows(IllegalArgumentException.class, () -> SequenceMatchDescription.fromColumns(columns(wrongMembers)));
    }

    public void testRejectsKeyRoleMismatchAndDuplicateKeys() {
        for (List<Map<String, Object>> keys : List.of(
            List.of(Map.<String, Object>of("name", "k", "type", "keyword", "column", "c1")),
            List.of(Map.<String, Object>of("name", "k", "type", "_source", "column", "c4")),
            List.of(key(), key())
        )) {
            Map<String, Object> contract = contract();
            contract.put("keys", keys);
            expectThrows(IllegalArgumentException.class, () -> SequenceMatchDescription.fromColumns(columns(contract)));
        }
    }

    public void testRejectsElidedWidthAndMalformedRetainedValues() {
        var description = SequenceMatchDescription.fromColumns(columns(contract()));
        expectThrows(
            IllegalArgumentException.class,
            () -> description.decodeRow(List.of("partial"), SequenceMatchDescription.SourcePolicy.ALLOW_NULL)
        );
        for (int slot : new int[] { 1, 2, 3, 4 }) {
            List<Object> values = new ArrayList<>(List.of("edited", 1L, "a", "A", Map.of(), "b", "B", Map.of()));
            values.set(slot, List.of("not-scalar"));
            expectThrows(
                IllegalArgumentException.class,
                () -> description.decodeRow(values, SequenceMatchDescription.SourcePolicy.ALLOW_NULL)
            );
        }
        List<Object> nullKey = Arrays.asList("edited", null, "a", "A", Map.of(), "b", "B", Map.of());
        expectThrows(
            IllegalArgumentException.class,
            () -> description.decodeRow(nullKey, SequenceMatchDescription.SourcePolicy.ALLOW_NULL)
        );
    }

    private static Map<String, Object> contract() {
        return new LinkedHashMap<>(
            Map.of(
                "version",
                1,
                "layout",
                "columns",
                "primary_stage",
                2,
                "member_count",
                2,
                "keys",
                List.of(key()),
                "members",
                List.of(member(1, "c2", "c3", "c4"), member(2, "c5", "c6", "c7"))
            )
        );
    }

    private static Map<String, Object> key() {
        return Map.of("name", "k", "type", "long", "column", "c1");
    }

    private static Map<String, Object> member(int stage, String index, String id, String source) {
        return Map.of("stage", stage, "index", index, "id", id, "source", source);
    }

    private static List<ColumnInfo> columns(Map<String, Object> contract) {
        List<ColumnInfo> columns = new ArrayList<>();
        columns.add(column("_id", DataType.KEYWORD, "c0", null));
        columns.add(column("same-display-name", DataType.LONG, "c1", null));
        columns.add(column("same-display-name", DataType.KEYWORD, "c2", contract));
        columns.add(column("same-display-name", DataType.KEYWORD, "c3", null));
        columns.add(column("same-display-name", DataType.SOURCE, "c4", null));
        columns.add(column("same-display-name", DataType.KEYWORD, "c5", null));
        columns.add(column("same-display-name", DataType.KEYWORD, "c6", null));
        columns.add(column("same-display-name", DataType.SOURCE, "c7", null));
        return columns;
    }

    private static ColumnInfo column(String name, DataType type, String id, Map<String, Object> contract) {
        Map<String, Object> role = new LinkedHashMap<>();
        role.put("column_id", id);
        if (contract != null) role.put("contract", contract);
        return new ColumnInfoImpl(name, type, null, Map.of("sequence", role));
    }
}
