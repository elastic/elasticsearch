/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Test consumer for self-identifying provenance values; input value order carries no meaning. */
public final class SequenceProvenance {
    private SequenceProvenance() {}

    /** Identifies one stage's genuine selected document, independently of ordinary output fields. */
    public record Member(int stage, String index, String id) {}

    /** Decode every entry by its own stage, index and id, then order only for deterministic assertions. */
    public static List<Member> decode(Object cell) {
        if (cell instanceof List<?> == false) throw new IllegalArgumentException("provenance must contain one entry per stage");
        List<Member> members = new ArrayList<>();
        Set<Integer> stages = new HashSet<>();
        for (Object value : (List<?>) cell) {
            if (value instanceof String == false) throw new IllegalArgumentException("provenance entry must be JSON keyword");
            try (var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, (String) value)) {
                var entry = parser.map();
                if (entry.keySet().equals(Set.of("stage", "index", "id")) == false
                    || entry.get("stage") instanceof Integer == false
                    || entry.get("index") instanceof String == false
                    || entry.get("id") instanceof String == false) {
                    throw new IllegalArgumentException("provenance entry must identify its stage, index and id");
                }
                int stage = (Integer) entry.get("stage");
                if (stage < 1 || stages.add(stage) == false) throw new IllegalArgumentException("duplicate or invalid provenance stage");
                members.add(new Member(stage, (String) entry.get("index"), (String) entry.get("id")));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        members.sort(Comparator.comparingInt(Member::stage));
        return List.copyOf(members);
    }
}
