/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

final class EqlViaEsql {

    static final List<String> CAPABILITIES = List.of("eql_command", "eql_command_metadata");

    private static final int ROW_LIMIT = 10_000;

    record Options(
        int size,
        int fetchSize,
        String resultPosition,
        String timestampField,
        String eventCategoryField,
        @Nullable String tiebreakerField,
        int maxSamplesPerKey,
        @Nullable Boolean allowPartialSequenceResults
    ) {}

    private EqlViaEsql() {}

    static ObjectPath runAndCompare(RestClient client, String index, String eqlQuery, Options options, String idField, ObjectPath eql)
        throws IOException {
        ObjectPath esql = run(client, index, eqlQuery, options, idField);
        assertEquals(
            "ES|QL EQL command diverges from _eql/search for query [" + eqlQuery + "]",
            summary(eql, idField),
            summary(esql, idField)
        );
        return esql;
    }

    private static ObjectPath run(RestClient client, String index, String eqlQuery, Options options, String idField) throws IOException {
        String esql = "EQL "
            + Arrays.stream(index.split(",")).map(i -> "\"" + i.trim() + "\"").collect(Collectors.joining(","))
            + " ?q METADATA _source WITH "
            + withOptions(options)
            + " | LIMIT "
            + ROW_LIMIT;
        XContentBuilder body = JsonXContent.contentBuilder()
            .startObject()
            .field("query", esql)
            .startArray("params")
            .startObject()
            .field("q", eqlQuery)
            .endObject()
            .endArray()
            .endObject();
        Request request = new Request("POST", "/_query");
        request.setJsonEntity(Strings.toString(body));
        ObjectPath response = ObjectPath.createFromResponse(client.performRequest(request));
        return toEqlResponse(response.evaluate("columns"), response.evaluate("values"), idField);
    }

    private static String withOptions(Options options) {
        StringJoiner with = new StringJoiner(", ", "{", "}");
        with.add("\"size\": " + options.size());
        with.add("\"fetch_size\": " + options.fetchSize());
        with.add("\"result_position\": \"" + options.resultPosition() + "\"");
        with.add("\"timestamp_field\": \"" + options.timestampField() + "\"");
        with.add("\"event_category_field\": \"" + options.eventCategoryField() + "\"");
        if (options.tiebreakerField() != null) {
            with.add("\"tiebreaker_field\": \"" + options.tiebreakerField() + "\"");
        }
        if (options.maxSamplesPerKey() > 0) {
            with.add("\"max_samples_per_key\": " + options.maxSamplesPerKey());
        }
        if (options.allowPartialSequenceResults() != null) {
            with.add("\"allow_partial_sequence_results\": " + options.allowPartialSequenceResults());
        }
        return with.toString();
    }

    private static ObjectPath toEqlResponse(List<Map<String, Object>> columns, List<List<Object>> rows, String idField) {
        List<String> names = columns.stream().map(c -> (String) c.get("name")).toList();
        int sourceColumn = names.indexOf("_source");
        int idColumn = names.indexOf(idField);
        int sequenceColumn = names.indexOf("_sequence");
        int joinKeysColumn = names.indexOf("join_keys");

        Map<String, Object> hits = new LinkedHashMap<>();
        if (sequenceColumn < 0) {
            List<Map<String, Object>> events = new ArrayList<>(rows.size());
            for (List<Object> row : rows) {
                events.add(event(row, sourceColumn, idColumn, idField));
            }
            hits.put("events", events);
        } else {
            Map<Object, List<Map<String, Object>>> sequences = new LinkedHashMap<>();
            Map<Object, Object> joinKeys = new LinkedHashMap<>();
            Object previous = null;
            for (List<Object> row : rows) {
                Object sequence = row.get(sequenceColumn);
                if (sequence.equals(previous) == false) {
                    assertFalse("rows of sequence [" + sequence + "] are not contiguous", sequences.containsKey(sequence));
                    sequences.put(sequence, new ArrayList<>());
                    joinKeys.put(sequence, row.get(joinKeysColumn));
                    previous = sequence;
                }
                sequences.get(sequence).add(event(row, sourceColumn, idColumn, idField));
            }
            List<Map<String, Object>> rebuilt = new ArrayList<>(sequences.size());
            sequences.forEach((sequence, events) -> {
                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("join_keys", asList(joinKeys.get(sequence)));
                entry.put("events", events);
                rebuilt.add(entry);
            });
            hits.put("sequences", rebuilt);
        }
        return new ObjectPath(Map.of("hits", hits));
    }

    private static List<?> asList(Object joinKeys) {
        if (joinKeys == null) {
            return List.of();
        }
        return joinKeys instanceof List<?> list ? list : List.of(joinKeys);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> event(List<Object> row, int sourceColumn, int idColumn, String idField) {
        Object source = row.get(sourceColumn);
        if (source == null) {
            return Map.of("missing", true);
        }
        assertTrue("no typed column [" + idField + "]", idColumn >= 0);
        assertEquals(
            "typed column [" + idField + "] differs from _source",
            idValue(((Map<String, Object>) source).get(idField)),
            idValue(row.get(idColumn))
        );
        return Map.of("_source", source);
    }

    private static List<String> summary(ObjectPath response, String idField) throws IOException {
        List<Map<String, Object>> events = response.evaluate("hits.events");
        if (events != null) {
            return events.stream().map(e -> eventId(e, idField)).toList();
        }
        List<Map<String, Object>> sequences = response.evaluate("hits.sequences");
        List<String> summary = new ArrayList<>(sequences.size());
        for (Map<String, Object> sequence : sequences) {
            List<?> keys = (List<?>) sequence.getOrDefault("join_keys", List.of());
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> sequenceEvents = (List<Map<String, Object>>) sequence.getOrDefault("events", List.of());
            summary.add(keys.stream().map(String::valueOf).toList() + " " + sequenceEvents.stream().map(e -> eventId(e, idField)).toList());
        }
        return summary;
    }

    @SuppressWarnings("unchecked")
    private static String eventId(Map<String, Object> event, String idField) {
        if (Boolean.TRUE.equals(event.get("missing"))) {
            return "missing";
        }
        return idValue(((Map<String, Object>) event.get("_source")).get(idField));
    }

    private static String idValue(Object id) {
        return id instanceof Number n ? Long.toString(n.longValue()) : String.valueOf(id);
    }
}
