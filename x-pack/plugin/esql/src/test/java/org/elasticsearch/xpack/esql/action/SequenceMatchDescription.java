/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Test reference reader for the prototype's version-1 retained-match columns, using their mandatory descriptor, never display names.
 * The caller must first admit a completed buffered response with the full column schema (no null-column elision).
 * This validates row association, not completeness, a rule cap, authorization, or durable event/match identity.
 */
public final class SequenceMatchDescription {
    private static final Set<String> KEY_TYPES = Set.of(
        "boolean",
        "integer",
        "long",
        "unsigned_long",
        "date",
        "date_nanos",
        "double",
        "keyword",
        "ip",
        "version",
        "null"
    );

    /** Source availability is a consumer decision; a decoder must never manufacture an absent original. */
    public enum SourcePolicy {
        ALLOW_NULL,
        REQUIRE_PRESENT
    }

    /** Column positions are response-local and cannot serve as durable member identities. */
    public record MemberColumns(int stage, int index, int id, int source) {}

    /** The source is Elasticsearch's caller-visible value, including its stored/synthetic-source behavior. */
    public record Member(int stage, String index, String id, @Nullable Map<String, Object> source) {}

    /** Values keep their existing Java types; in particular, integer evidence is never converted to double. */
    public record Row(List<Object> ordinary, List<Object> keys, List<Member> members) {}

    private final int primary;
    private final int width;
    private final List<String> ordinaryNames;
    private final List<Integer> ordinary;
    private final List<Integer> keys;
    private final List<MemberColumns> members;

    private SequenceMatchDescription(
        int primary,
        List<? extends ColumnInfo> columns,
        Set<Integer> retained,
        List<Integer> keys,
        List<MemberColumns> members
    ) {
        this.primary = primary;
        this.width = columns.size();
        List<Integer> ordinary = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            if (retained.contains(i) == false) ordinary.add(i);
        }
        this.ordinary = List.copyOf(ordinary);
        this.ordinaryNames = ordinary.stream().map(i -> columns.get(i).name()).toList();
        this.keys = List.copyOf(keys);
        this.members = List.copyOf(members);
    }

    /** Rejects missing/ambiguous descriptors and references before any rows, including an empty result, are interpreted. */
    public static SequenceMatchDescription fromColumns(List<? extends ColumnInfo> columns) {
        Map<String, Integer> ids = new LinkedHashMap<>();
        Map<String, Object> contract = null;
        int anchor = -1;
        for (int i = 0; i < columns.size(); i++) {
            var role = object(object(columns.get(i).meta()).get("sequence"));
            String id = text(role.get("column_id"));
            if (ids.put(id, i) != null) throw invalid("duplicate column ID");
            if (role.containsKey("contract")) {
                if (contract != null) throw invalid("multiple descriptors");
                contract = object(role.get("contract"));
                anchor = i;
            }
        }
        if (contract == null
            || Integer.valueOf(1).equals(contract.get("version")) == false
            || "columns".equals(contract.get("layout")) == false) throw invalid("unsupported descriptor version or layout");
        int primary = integer(contract.get("primary_stage"));
        List<?> declaredMembers = array(contract.get("members"));
        if (declaredMembers.size() < 2
            || primary < 1
            || primary > declaredMembers.size()
            || integer(contract.get("member_count")) != declaredMembers.size()) throw invalid("invalid primary/member count");
        Set<Integer> retained = new HashSet<>();
        List<Integer> keys = new ArrayList<>();
        for (Object item : array(contract.get("keys"))) {
            var key = object(item);
            text(key.get("name"));
            String type = text(key.get("type"));
            if (KEY_TYPES.contains(type) == false) throw invalid("unsupported key type");
            keys.add(channel(ids, columns, key.get("column"), type, retained));
        }
        List<MemberColumns> members = new ArrayList<>();
        for (Object item : declaredMembers) {
            var member = object(item);
            int stage = integer(member.get("stage"));
            if (stage != members.size() + 1) throw invalid("unordered member stages");
            members.add(
                new MemberColumns(
                    stage,
                    channel(ids, columns, member.get("index"), "keyword", retained),
                    channel(ids, columns, member.get("id"), "keyword", retained),
                    channel(ids, columns, member.get("source"), "_source", retained)
                )
            );
        }
        if (members.getFirst().index() != anchor) throw invalid("invalid descriptor anchor");
        return new SequenceMatchDescription(primary, columns, retained, keys, members);
    }

    /** Decodes one full-width row without coercion; a strict evidence consumer can reject unavailable source. */
    public Row decodeRow(List<Object> values, SourcePolicy sourcePolicy) {
        if (sourcePolicy == null) throw invalid("source policy required");
        if (values.size() != width) throw invalid("row/schema width mismatch");
        List<Object> keyValues = keys.stream().map(values::get).toList();
        for (Object value : keyValues) {
            if (value == null || value instanceof List<?> || value instanceof Map<?, ?>) throw invalid("non-scalar/null match key");
        }
        List<Member> originals = new ArrayList<>();
        for (MemberColumns member : members) {
            Object value = values.get(member.source());
            if (value == null && sourcePolicy == SourcePolicy.REQUIRE_PRESENT) throw invalid("member source is unavailable");
            originals.add(
                new Member(
                    member.stage(),
                    text(values.get(member.index())),
                    text(values.get(member.id())),
                    value == null ? null : object(value)
                )
            );
        }
        return new Row(ordinary.stream().map(values::get).toList(), keyValues, List.copyOf(originals));
    }

    /** The primary original comes from its retained member, independently of editable ordinary fields. */
    public Member primary(Row row) {
        if (row.members().size() != members.size()) throw invalid("row/member count mismatch");
        return row.members().get(primary - 1);
    }

    public int primaryStage() {
        return primary;
    }

    public List<String> ordinaryNames() {
        return ordinaryNames;
    }

    public List<MemberColumns> members() {
        return members;
    }

    private static int channel(Map<String, Integer> ids, List<? extends ColumnInfo> columns, Object id, String type, Set<Integer> seen) {
        Integer slot = ids.get(text(id));
        if (slot == null || columns.get(slot).outputType().equals(type) == false || seen.add(slot) == false) {
            throw invalid("missing, duplicated or wrongly typed role");
        }
        return slot;
    }

    private static String text(Object value) {
        if (value instanceof String text && text.isEmpty() == false) return text;
        throw invalid("nonempty string required");
    }

    private static int integer(Object value) {
        if (value instanceof Integer integer) return integer;
        throw invalid("integer required");
    }

    private static List<?> array(Object value) {
        if (value instanceof List<?> list) return list;
        throw invalid("array required");
    }

    private static Map<String, Object> object(Object value) {
        if ((value instanceof Map<?, ?>) == false) throw invalid("object required");
        Map<String, Object> result = new LinkedHashMap<>();
        for (var entry : ((Map<?, ?>) value).entrySet()) {
            if ((entry.getKey() instanceof String) == false) throw invalid("string object key required");
            result.put((String) entry.getKey(), entry.getValue());
        }
        return result;
    }

    private static IllegalArgumentException invalid(String message) {
        return new IllegalArgumentException("Invalid SEQUENCE match response: " + message);
    }
}
