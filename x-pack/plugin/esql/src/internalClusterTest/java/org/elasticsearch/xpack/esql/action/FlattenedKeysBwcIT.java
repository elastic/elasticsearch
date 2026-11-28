/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

/**
 * A differential test for whether keys of {@code flattened} fields change what queries meant before keys existed.
 * <p>
 * Every query of a generated matrix runs twice on the same data: once with the pragma {@code flattened_keys=false}, which is the
 * behavior from before keys, and once as it runs now. A query that fails without keys is free to mean anything. A query that
 * succeeds without keys must return the same with them, with two exceptions that are deliberate:
 * <ul>
 *     <li>a {@code null} may become a value, where a name that nothing answered is now a key;</li>
 *     <li>the type {@code null} of such a column may become {@code keyword}.</li>
 * </ul>
 * Everything else is a break: a value that changes, a column that appears or disappears, a query that now fails.
 * <p>
 * The matrix places a {@code flattened} field {@code attrs} wherever it can meet a name {@code attrs.<x>} that already meant
 * something: a mapped field of another index, a field that only {@code _source} holds, a column that the query computes, and a
 * map that a lookup index brings in. It does not cover {@code ENRICH} and views yet.
 */
public class FlattenedKeysBwcIT extends AbstractEsqlIntegTestCase {

    private static final Logger logger = LogManager.getLogger(FlattenedKeysBwcIT.class);

    private enum Outcome {
        /** fails without keys: not a released query */
        FREE,
        SAME,
        NULL_TO_VALUE,
        /** an expression over a name whose null became a value, or whose null type became keyword */
        FOLLOWS_FROM_NULL_TO_VALUE,
        NULL_TYPE_TO_KEYWORD,
        BREAK_NOW_FAILS,
        BREAK_VALUE_CHANGED,
        BREAK_COLUMNS_CHANGED
    }

    private static final List<String> MODES = List.of("", "nullify", "load", "load_all");

    private static final List<String> SOURCES = List.of(
        "kw",
        "flat",
        "src",
        "plain",
        "kw, flat",
        "kw, src",
        "flat, src",
        "plain, flat",
        "kw, flat, src",
        "deep, flat"
    );

    /** Commands between the source and the read. They keep {@code _index} and {@code id}, which identify a row. */
    private static final List<String> MIDDLES = List.of(
        "",
        "| LOOKUP JOIN lk ON id",
        "| LOOKUP JOIN lk_kw ON id",
        "| RENAME attrs AS a",
        "| EVAL a = attrs",
        "| EVAL attrs = \"x\"",
        "| EVAL attrs.host = \"computed\"",
        "| EVAL a.host = \"computed\" | RENAME attrs AS a",
        "| KEEP _index, id, attrs",
        "| KEEP _index, id, attrs*",
        "| DROP attrs",
        "| RENAME attrs.host AS h",
        "| LOOKUP JOIN lk ON id | RENAME attrs AS a",
        "| EVAL attrs.host = \"computed\" | LOOKUP JOIN lk ON id"
    );

    private static final String DERIVED = " | EVAL r = /*derived*/ ";

    private static final List<String> NAMES = List.of("attrs.host", "attrs.zone", "attrs.count", "a.host", "a.zone", "attrs.sub.x");

    @Before
    public void createIndices() {
        createIndex("kw", false, """
            "id": {"type": "integer"}, "attrs": {"properties": {"host": {"type": "keyword"}, "count": {"type": "long"}}}""");
        createIndex("flat", false, """
            "id": {"type": "integer"}, "attrs": {"type": "flattened"}""");
        createIndex("deep", false, """
            "id": {"type": "integer"}, "attrs": {"properties": {"sub": {"type": "flattened"}}}""");
        createIndex("plain", false, """
            "id": {"type": "integer"}, "name": {"type": "keyword"}""");
        createIndex("lk", true, """
            "id": {"type": "integer"}, "attrs": {"type": "flattened"}""");
        createIndex("lk_kw", true, """
            "id": {"type": "integer"}, "attrs": {"properties": {"host": {"type": "keyword"}}}""");
        // dynamic: false, so that what is under [attrs] and [a] is only in _source
        assertAcked(
            client().admin().indices().prepareCreate("src").setSettings(Settings.builder().put("index.number_of_shards", 1)).setMapping("""
                {"dynamic": false, "properties": {"id": {"type": "integer"}}}""")
        );

        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        index(bulk, "kw", """
            {"id": 1, "attrs": {"host": "k1", "count": 5}}""");
        index(bulk, "kw", """
            {"id": 2, "attrs": {"host": "k2", "count": 7}}""");
        index(bulk, "flat", """
            {"id": 1, "attrs": {"host": "f1", "zone": "fz1", "count": "9", "sub": {"x": "fx1"}}}""");
        index(bulk, "flat", """
            {"id": 3, "attrs": {"host": "f3", "zone": "fz3"}}""");
        index(bulk, "deep", """
            {"id": 1, "attrs": {"sub": {"x": "d1"}}}""");
        index(bulk, "plain", """
            {"id": 1, "name": "p1"}""");
        index(bulk, "plain", """
            {"id": 4, "name": "p4"}""");
        index(bulk, "src", """
            {"id": 1, "attrs": {"host": "s1", "zone": "sz1", "count": 3}, "a": {"host": "literal"}}""");
        index(bulk, "src", """
            {"id": 5, "attrs": {"host": "s5"}}""");
        index(bulk, "lk", """
            {"id": 1, "attrs": {"host": "L1", "zone": "LZ1"}}""");
        index(bulk, "lk", """
            {"id": 2, "attrs": {"host": "L2"}}""");
        index(bulk, "lk_kw", """
            {"id": 1, "attrs": {"host": "LK1"}}""");
        assertFalse(bulk.get().hasFailures());
    }

    private void createIndex(String name, boolean lookup, String properties) {
        Settings.Builder settings = Settings.builder().put("index.number_of_shards", 1);
        if (lookup) {
            settings.put("index.mode", "lookup");
        }
        assertAcked(
            client().admin().indices().prepareCreate(name).setSettings(settings).setMapping("{\"properties\": {" + properties + "}}")
        );
    }

    private static void index(BulkRequestBuilder bulk, String index, String source) {
        bulk.add(new IndexRequest(index).source(source, XContentType.JSON));
    }

    public void testQueriesMeanWhatTheyMeantWithoutKeys() {
        assumeTrue("requires keys of flattened fields", EsqlCapabilities.Cap.FLATTENED_KEYS.isEnabled());
        Map<String, Map<Outcome, Integer>> summary = new TreeMap<>();
        List<String> breaks = new ArrayList<>();
        List<String> nullsToValues = new ArrayList<>();
        List<String> follows = new ArrayList<>();
        int followsByFailing = 0;
        for (String mode : MODES) {
            Map<Outcome, Integer> counts = new EnumMap<>(Outcome.class);
            summary.put(mode.isEmpty() ? "default" : mode, counts);
            Outcome plainRead = null;
            for (String query : queries(mode)) {
                Result without = run(query, false);
                Result with = run(query, true);
                StringBuilder detail = new StringBuilder();
                Outcome outcome = compare(without, with, detail);
                if (query.contains(DERIVED)) {
                    // The plain read of the same name comes right before its derived uses. Where that read went from null to a
                    // value or from the null type to keyword, an expression over it changes with it, or stops type checking.
                    boolean readChanged = plainRead == Outcome.NULL_TO_VALUE || plainRead == Outcome.NULL_TYPE_TO_KEYWORD;
                    if (readChanged && (outcome == Outcome.BREAK_VALUE_CHANGED || outcome == Outcome.BREAK_NOW_FAILS)) {
                        if (outcome == Outcome.BREAK_NOW_FAILS) {
                            followsByFailing++;
                        }
                        detail.insert(0, "(" + outcome + ") ");
                        outcome = Outcome.FOLLOWS_FROM_NULL_TO_VALUE;
                        follows.add(query + "\n    " + detail);
                    }
                } else {
                    plainRead = outcome;
                }
                counts.merge(outcome, 1, Integer::sum);
                if (outcome.name().startsWith("BREAK")) {
                    breaks.add(outcome + ": " + query + "\n    " + detail);
                } else if (outcome == Outcome.NULL_TO_VALUE) {
                    nullsToValues.add(query + "\n    " + detail);
                }
            }
        }
        StringBuilder report = new StringBuilder("flattened keys bwc summary:\n");
        summary.forEach((mode, counts) -> report.append(String.format(Locale.ROOT, "  %-8s %s%n", mode, counts)));
        report.append("nulls that became values (").append(nullsToValues.size()).append("), first 40:\n");
        nullsToValues.stream().limit(40).forEach(q -> report.append("  ").append(q).append('\n'));
        report.append("follow from a null that became a value (")
            .append(follows.size())
            .append(", of which ")
            .append(followsByFailing)
            .append(" no longer type check), first 40:\n");
        follows.stream().limit(40).forEach(q -> report.append("  ").append(q).append('\n'));
        report.append("breaks (").append(breaks.size()).append("):\n");
        breaks.forEach(b -> report.append("  ").append(b).append('\n'));
        logger.info(report.toString());
        assertTrue(report.toString(), breaks.isEmpty());
    }

    private static List<String> queries(String mode) {
        String set = mode.isEmpty() ? "" : "SET unmapped_fields=\"" + mode + "\"; ";
        List<String> queries = new ArrayList<>();
        for (String source : SOURCES) {
            for (String middle : MIDDLES) {
                String head = set + "FROM " + source + " METADATA _index " + middle;
                String tail = " | SORT _index, id | LIMIT 100";
                // every column: a key must not show up as one
                queries.add(head + tail);
                for (String name : NAMES) {
                    queries.add(head + " | KEEP _index, id, " + name + tail);
                    queries.add(head + " | EVAL r = " + name + " | KEEP _index, id, r" + tail);
                    // typed uses, which a null accepts and a keyword does not
                    queries.add(head + DERIVED + name + " > 4 | KEEP _index, id, r" + tail);
                    queries.add(head + DERIVED + "COALESCE(" + name + ", 0) | KEEP _index, id, r" + tail);
                }
            }
        }
        return queries;
    }

    private record Result(List<String> names, List<String> types, List<List<Object>> rows, String error) {}

    private Result run(String query, boolean keys) {
        EsqlQueryRequest request = syncEsqlQueryRequest(query);
        request.pragmas(new QueryPragmas(Settings.builder().put(QueryPragmas.FLATTENED_KEYS.getKey(), keys).build()));
        try (EsqlQueryResponse response = run(request)) {
            List<String> names = new ArrayList<>();
            List<String> types = new ArrayList<>();
            for (ColumnInfoImpl column : response.columns()) {
                names.add(column.name());
                types.add(column.outputType());
            }
            return new Result(names, types, getValuesList(response), null);
        } catch (Exception e) {
            return new Result(null, null, null, String.valueOf(e.getMessage()));
        }
    }

    private static Outcome compare(Result without, Result with, StringBuilder detail) {
        if (without.error != null) {
            return Outcome.FREE;
        }
        if (with.error != null) {
            detail.append("now fails: ").append(firstLine(with.error));
            return Outcome.BREAK_NOW_FAILS;
        }
        if (without.names.equals(with.names) == false) {
            detail.append("columns ").append(without.names).append(" -> ").append(with.names);
            return Outcome.BREAK_COLUMNS_CHANGED;
        }
        boolean typeChanged = false;
        for (int c = 0; c < without.types.size(); c++) {
            String before = without.types.get(c);
            String after = with.types.get(c);
            if (before.equals(after) == false) {
                if (before.equals("null") && after.equals("keyword")) {
                    typeChanged = true;
                } else {
                    detail.append("type of [").append(without.names.get(c)).append("] ").append(before).append(" -> ").append(after);
                    return Outcome.BREAK_COLUMNS_CHANGED;
                }
            }
        }
        if (without.rows.size() != with.rows.size()) {
            detail.append("rows ").append(without.rows.size()).append(" -> ").append(with.rows.size());
            return Outcome.BREAK_VALUE_CHANGED;
        }
        boolean nullToValue = false;
        for (int r = 0; r < without.rows.size(); r++) {
            List<Object> before = without.rows.get(r);
            List<Object> after = with.rows.get(r);
            for (int c = 0; c < before.size(); c++) {
                if (Objects.equals(before.get(c), after.get(c))) {
                    continue;
                }
                if (before.get(c) == null) {
                    if (nullToValue == false) {
                        detail.append(before).append(" -> ").append(after);
                    }
                    nullToValue = true;
                } else {
                    detail.setLength(0);
                    detail.append("row ").append(before).append(" -> ").append(after);
                    return Outcome.BREAK_VALUE_CHANGED;
                }
            }
        }
        if (nullToValue) {
            return Outcome.NULL_TO_VALUE;
        }
        return typeChanged ? Outcome.NULL_TYPE_TO_KEYWORD : Outcome.SAME;
    }

    private static String firstLine(String message) {
        String line = message.replace("\n", " / ");
        return line.length() > 300 ? line.substring(0, 300) : line;
    }
}
