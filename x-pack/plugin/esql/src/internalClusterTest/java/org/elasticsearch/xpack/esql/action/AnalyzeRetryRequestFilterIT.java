/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

public class AnalyzeRetryRequestFilterIT extends AbstractEsqlIntegTestCase {

    private void createIndex(String name, String mappingJson) {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(name)
                .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .setMapping(mappingJson)
        );
    }

    private void createIndex(String name, String field, String type) {
        createIndex(name, "{ \"properties\": { \"" + field + "\": { \"type\": \"" + type + "\" } } }");
    }

    private EsqlQueryResponse runFiltered(String mode, String body, QueryBuilder filter) {
        String query = mode == null ? body : "SET unmapped_fields=\"" + mode + "\";\n" + body;
        EsqlQueryRequest request = syncEsqlQueryRequest(query);
        request.filter(filter);
        return run(request);
    }

    private static List<String> names(List<? extends ColumnInfo> columns) {
        return columns.stream().map(ColumnInfo::name).toList();
    }

    private void indexJson(String index, String id, String json) {
        client().prepareIndex(index).setId(id).setSource(json, XContentType.JSON).get();
    }

    /**
     * A field mapped only in the filter-pruned index: DEFAULT retries and reports its real type ({@code integer}); NULLIFY reports
     * {@code null}, LOAD reports {@code keyword}. Column names and row data are identical across modes; only the pruned field's type
     * diverges.
     *
     * field_caps:
     * DEFAULT      makes the second, unfiltered call ({@code id2} fails first-pass analysis → retry);
     * NULLIFY/LOAD resolve {@code id2} on the first pass and make a single call
     */
    public void testPrunedOnlyFieldColumnTypeDiverges() {
        assumeTrue("requires nullify + load", nullifyEnabled() && loadEnabled());
        createIndex("test1", "id1", "integer");
        createIndex("test2", "id2", "integer");
        indexDoc("test1", "a", "id1", 0);
        indexDoc("test1", "b", "id1", 1);
        indexDoc("test1", "c", "id1", 2);
        indexDoc("test2", "x", "id2", 10);
        indexDoc("test2", "y", "id2", 11);
        refresh("test1", "test2");

        String body = "FROM test1,test2 | KEEP id1, id2 | SORT id1";
        QueryBuilder filter = QueryBuilders.existsQuery("id1"); // prunes test2 (no id1) from field_caps and from execution

        List<List<Object>> expectedValues = List.of(Arrays.asList(0, null), Arrays.asList(1, null), Arrays.asList(2, null));

        try (var def = runFiltered(null, body, filter)) {
            assertThat(def.isPartial(), equalTo(false));
            assertColumnNames(def.columns(), List.of("id1", "id2"));
            assertColumnTypes(def.columns(), List.of("integer", "integer"));
            assertThat(getValuesList(def), equalTo(expectedValues));
        }
        try (var nullify = runFiltered("nullify", body, filter)) {
            assertColumnNames(nullify.columns(), List.of("id1", "id2"));
            assertColumnTypes(nullify.columns(), List.of("integer", "null"));
            assertThat(getValuesList(nullify), equalTo(expectedValues));
        }
        try (var load = runFiltered("load", body, filter)) {
            assertColumnNames(load.columns(), List.of("id1", "id2"));
            assertColumnTypes(load.columns(), List.of("integer", "keyword"));
            assertThat(getValuesList(load), equalTo(expectedValues));
        }
    }

    /**
     * A type-conflicted field ({@code id}: integer in one index, keyword in another) whose every mapping is pruned by the filter, while a
     * third index that does not map it survives. DEFAULT retries, sees both mappings and reports {@code unsupported} (union conflict);
     * NULLIFY reports {@code null} and LOAD reports {@code keyword} — silently masking the conflict.
     *
     * field_caps:
     * DEFAULT      makes the second, unfiltered call (id fails first-pass analysis → retry);
     * NULLIFY/LOAD resolve it and make a single call
     */
    public void testTypeConflictHiddenByFilterIsMasked() {
        assumeTrue("requires nullify + load", nullifyEnabled() && loadEnabled());
        createIndex("t_int", "id", "integer");
        createIndex("t_kw", "id", "keyword");
        createIndex("t_other", "val3", "integer");
        indexDoc("t_int", "a", "id", 100);
        indexDoc("t_kw", "a", "id", "kw");
        indexDoc("t_other", "a", "val3", 0);
        indexDoc("t_other", "b", "val3", 1);
        indexDoc("t_other", "c", "val3", 2);
        refresh("t_int", "t_kw", "t_other");

        String body = "FROM t_int,t_kw,t_other | KEEP val3, id | SORT val3";
        QueryBuilder filter = QueryBuilders.existsQuery("val3"); // prunes t_int and t_kw, keeps t_other

        List<List<Object>> expectedValues = List.of(Arrays.asList(0, null), Arrays.asList(1, null), Arrays.asList(2, null));

        try (var def = runFiltered(null, body, filter)) {
            assertThat(def.isPartial(), equalTo(false));
            assertColumnNames(def.columns(), List.of("val3", "id"));
            assertColumnTypes(def.columns(), List.of("integer", "unsupported"));
            assertThat(getValuesList(def), equalTo(expectedValues));
        }
        try (var nullify = runFiltered("nullify", body, filter)) {
            assertColumnNames(nullify.columns(), List.of("val3", "id"));
            assertColumnTypes(nullify.columns(), List.of("integer", "null"));
            assertThat(getValuesList(nullify), equalTo(expectedValues));
        }
        try (var load = runFiltered("load", body, filter)) {
            assertColumnNames(load.columns(), List.of("val3", "id"));
            assertColumnTypes(load.columns(), List.of("integer", "keyword"));
            assertThat(getValuesList(load), equalTo(expectedValues));
        }
    }

    /**
     * LOAD's keyword assumption can mask a downstream type error that DEFAULT surfaces: {@code SUBSTRING} is valid on the assumed
     * keyword but invalid on the pruned field's real {@code integer} type, so LOAD succeeds while DEFAULT (which retries to the real
     * type) fails verification.
     *
     * field_caps:
     * DEFAULT makes the second, unfiltered call (id2 fails first-pass analysis → retry, then errors on SUBSTRING type incompatibility);
     * LOAD    resolves id2 as keyword and never retries - SUBSTRING is happy from data type POV.
     */
    public void testLoadMasksDownstreamTypeErrorThatDefaultRaises() {
        assumeTrue("requires load", loadEnabled());
        createIndex("test1", "id1", "integer");
        createIndex("test2", "id2", "integer");
        indexDoc("test1", "a", "id1", 0);
        indexDoc("test1", "b", "id1", 1);
        indexDoc("test2", "x", "id2", 10);
        refresh("test1", "test2");

        String body = "FROM test1,test2 | EVAL s = SUBSTRING(id2, 1, 1) | KEEP id1, s | SORT id1";
        QueryBuilder filter = QueryBuilders.existsQuery("id1");

        try (var load = runFiltered("load", body, filter)) {
            assertThat(load.isPartial(), equalTo(false));
            assertColumnNames(load.columns(), List.of("id1", "s"));
            assertColumnTypes(load.columns(), List.of("integer", "keyword"));
            assertThat(getValuesList(load), equalTo(List.of(Arrays.asList(0, null), Arrays.asList(1, null))));
        }

        Exception e = expectThrows(Exception.class, () -> runFiltered(null, body, filter).close());
        assertThat(
            e.getMessage(),
            containsString("first argument of [SUBSTRING(id2, 1, 1)] must be [string], found value [id2] type [integer]")
        );
    }

    /**
     * NULLIFY masks the same downstream error: {@code SUBSTRING} is accepted on the {@code null} type, so NULLIFY succeeds while DEFAULT
     * retries to {@code integer} and fails verification.
     *
     * field_caps:
     * DEFAULT makes the second, unfiltered call id2 fails first-pass analysis → retry, then errors on SUBSTRING(integer);
     * NULLIFY resolves id2 as null and never retries.
     */
    public void testNullifyMasksDownstreamTypeErrorThatDefaultRaises() {
        assumeTrue("requires nullify", nullifyEnabled());
        createIndex("test1", "id1", "integer");
        createIndex("test2", "id2", "integer");
        indexDoc("test1", "a", "id1", 0);
        indexDoc("test1", "b", "id1", 1);
        indexDoc("test2", "x", "id2", 10);
        refresh("test1", "test2");

        String body = "FROM test1,test2 | EVAL s = SUBSTRING(id2, 1, 1) | KEEP id1, s | SORT id1";
        QueryBuilder filter = QueryBuilders.existsQuery("id1");

        try (var nullify = runFiltered("nullify", body, filter)) {
            assertThat(nullify.isPartial(), equalTo(false));
            assertColumnNames(nullify.columns(), List.of("id1", "s"));
            assertColumnTypes(nullify.columns(), List.of("integer", "keyword"));
            assertThat(getValuesList(nullify), equalTo(List.of(Arrays.asList(0, null), Arrays.asList(1, null))));
        }

        Exception e = expectThrows(Exception.class, () -> runFiltered(null, body, filter).close());
        assertThat(
            e.getMessage(),
            containsString("first argument of [SUBSTRING(id2, 1, 1)] must be [string], found value [id2] type [integer]")
        );
    }

    /**
     * Arithmetic on the pruned-only field is consistent across all three modes, each for a different reason:
     * DEFAULT fails to resolve id2, retries without the filter and types it {@code integer}
     * LOAD's  keyword assumption makes id2 * 2 invalid, so it throws and the same VerificationException retry re-resolves it to integer
     * NULLIFY accepts null * 2 and widens the derived column to integer via the non-null operand. All three yield d:integer.
     *
     *  field_caps:
     *  DEFAULT and LOAD both make the second, unfiltered call (LOAD via the keyword*2 self-heal retry);
     *  NULLIFY          resolves on the first pass and makes a single call.
     */
    public void testArithmeticOnPrunedFieldIsConsistentAcrossModes() {
        assumeTrue("requires nullify + load", nullifyEnabled() && loadEnabled());
        createIndex("test1", "id1", "integer");
        createIndex("test2", "id2", "integer");
        indexDoc("test1", "a", "id1", 0);
        indexDoc("test1", "b", "id1", 1);
        indexDoc("test2", "x", "id2", 10);
        refresh("test1", "test2");

        String body = "FROM test1,test2 | EVAL d = id2 * 2 | KEEP id1, d | SORT id1";
        QueryBuilder filter = QueryBuilders.existsQuery("id1");

        List<List<Object>> expectedValues = List.of(Arrays.asList(0, null), Arrays.asList(1, null));

        try (var def = runFiltered(null, body, filter)) {
            assertThat(def.isPartial(), equalTo(false));
            assertColumnNames(def.columns(), List.of("id1", "d"));
            assertColumnTypes(def.columns(), List.of("integer", "integer"));
            assertThat(getValuesList(def), equalTo(expectedValues));
        }
        try (var load = runFiltered("load", body, filter)) {
            assertColumnNames(load.columns(), List.of("id1", "d"));
            assertColumnTypes(load.columns(), List.of("integer", "integer"));
            assertThat(getValuesList(load), equalTo(expectedValues));
        }
        try (var nullify = runFiltered("nullify", body, filter)) {
            assertColumnNames(nullify.columns(), List.of("id1", "d"));
            assertColumnTypes(nullify.columns(), List.of("integer", "integer"));
            assertThat(getValuesList(nullify), equalTo(expectedValues));
        }
    }

    /**
     * Missing-columns divergence via a wildcard projection. id2 (pruned-only) forces DEFAULT to retry without the filter, so
     * the wildcard * re-expands over the pruned index and its other field extraB appears; NULLIFY/LOAD don't retry, so extraB is absent
     * from the output. The column SET differs across modes, not just a type.
     *
     * field_caps:
     * DEFAULT      makes the second, unfiltered call (id2 fails first-pass analysis → retry that re-expands *);
     * NULLIFY/LOAD resolve on the first pass and make a single call.
     */
    public void testWildcardProjectionMissingColumnsUnderNullifyLoad() {
        assumeTrue("requires nullify + load", nullifyEnabled() && loadEnabled());
        createIndex("test1", "{ \"properties\": { \"id1\": { \"type\": \"integer\" }, \"extraA\": { \"type\": \"integer\" } } }");
        createIndex("test2", "{ \"properties\": { \"id2\": { \"type\": \"integer\" }, \"extraB\": { \"type\": \"integer\" } } }");
        indexDoc("test1", "a", "id1", 0, "extraA", 5);
        indexDoc("test2", "x", "id2", 10, "extraB", 7);
        refresh("test1", "test2");

        String body = "FROM test1,test2 | KEEP id2, *";
        QueryBuilder filter = QueryBuilders.existsQuery("id1");

        try (var def = runFiltered(null, body, filter)) {
            assertThat(def.isPartial(), equalTo(false));
            List<String> cols = names(def.columns());
            assertThat(cols, hasItem("id1"));
            assertThat(cols, hasItem("extraA"));
            assertThat(cols, hasItem("id2"));
            assertThat(cols, hasItem("extraB")); // DEFAULT retry re-expands * over the pruned index
        }
        try (var nullify = runFiltered("nullify", body, filter)) {
            List<String> cols = names(nullify.columns());
            assertThat(cols, hasItem("id2"));
            assertThat(cols, not(hasItem("extraB"))); // no retry -> pruned index's extra field never surfaces
        }
        try (var load = runFiltered("load", body, filter)) {
            List<String> cols = names(load.columns());
            assertThat(cols, hasItem("id2"));
            assertThat(cols, not(hasItem("extraB")));
        }
    }

    /**
     * Row-DATA divergence. g is integer in the surviving index and keyword in the pruned one. Referencing the pruned-only id2 forces
     * DEFAULT to retry without the filter, which unions g into an unsupported conflict, so its real integer values render as null.
     * NULLIFY/LOAD don't retry, keep g as integer, and return its real values — the actual result data differs across modes, not just
     * the type.
     *
     * field_caps:
     * DEFAULT      makes the second, unfiltered call (id2 fails first-pass analysis → the retry is what makes g as unsupported type);
     * NULLIFY/LOAD resolve on the first pass and make a single call.
     */
    public void testSurvivingFieldContaminatedToUnsupportedByRetryChangesData() {
        assumeTrue("requires nullify + load", nullifyEnabled() && loadEnabled());
        createIndex("test1", "{ \"properties\": { \"id1\": { \"type\": \"integer\" }, \"g\": { \"type\": \"integer\" } } }");
        createIndex("test2", "{ \"properties\": { \"id2\": { \"type\": \"integer\" }, \"g\": { \"type\": \"keyword\" } } }");
        indexDoc("test1", "a", "id1", 0, "g", 5);
        indexDoc("test1", "b", "id1", 1, "g", 6);
        indexDoc("test2", "x", "id2", 10, "g", "kw");
        refresh("test1", "test2");

        String body = "FROM test1,test2 | KEEP id1, g, id2 | SORT id1";
        QueryBuilder filter = QueryBuilders.existsQuery("id1");

        try (var def = runFiltered(null, body, filter)) {
            assertThat(def.isPartial(), equalTo(false));
            assertColumnNames(def.columns(), List.of("id1", "g", "id2"));
            assertColumnTypes(def.columns(), List.of("integer", "unsupported", "integer"));
            assertThat(getValuesList(def), equalTo(List.of(Arrays.asList(0, null, null), Arrays.asList(1, null, null))));
        }
        try (var nullify = runFiltered("nullify", body, filter)) {
            assertColumnNames(nullify.columns(), List.of("id1", "g", "id2"));
            assertColumnTypes(nullify.columns(), List.of("integer", "integer", "null"));
            assertThat(getValuesList(nullify), equalTo(List.of(Arrays.asList(0, 5, null), Arrays.asList(1, 6, null))));
        }
        try (var load = runFiltered("load", body, filter)) {
            assertColumnNames(load.columns(), List.of("id1", "g", "id2"));
            assertColumnTypes(load.columns(), List.of("integer", "integer", "keyword"));
            assertThat(getValuesList(load), equalTo(List.of(Arrays.asList(0, 5, null), Arrays.asList(1, 6, null))));
        }
    }

    /**
     * LOAD returns data that DEFAULT/NULLIFY report as null. The surviving index stores f in _source but does not map it (dynamic:false);
     * the pruned index maps f as integer. LOAD resolves f as an unmapped keyword and loads its real values from the surviving index's
     * _source; DEFAULT retries and types f as integer but cannot load it (the surviving index doesn't map it and DEFAULT doesn't read
     * _source); NULLIFY types it null.
     *
     * field_caps:
     * DEFAULT      makes the second, unfiltered call (f fails first-pass analysis on the dynamic:false survivor → retry);
     * NULLIFY/LOAD resolve on the first pass and make a single call.
     */
    public void testLoadLoadsPrunedFieldFromSourceWhileDefaultReportsNull() {
        assumeTrue("requires nullify + load", nullifyEnabled() && loadEnabled());
        createIndex("test1", "{ \"dynamic\": false, \"properties\": { \"id1\": { \"type\": \"integer\" } } }");
        createIndex("test2", "{ \"properties\": { \"f\": { \"type\": \"integer\" } } }");
        indexDoc("test1", "a", "id1", 0, "f", "7");
        indexDoc("test1", "b", "id1", 1, "f", "8");
        indexDoc("test2", "x", "f", 99);
        refresh("test1", "test2");

        String body = "FROM test1,test2 | KEEP id1, f | SORT id1";
        QueryBuilder filter = QueryBuilders.existsQuery("id1");

        try (var def = runFiltered(null, body, filter)) {
            assertThat(def.isPartial(), equalTo(false));
            assertColumnNames(def.columns(), List.of("id1", "f"));
            assertColumnTypes(def.columns(), List.of("integer", "integer"));
            assertThat(getValuesList(def), equalTo(List.of(Arrays.asList(0, null), Arrays.asList(1, null))));
        }
        try (var nullify = runFiltered("nullify", body, filter)) {
            assertColumnNames(nullify.columns(), List.of("id1", "f"));
            assertColumnTypes(nullify.columns(), List.of("integer", "null"));
            assertThat(getValuesList(nullify), equalTo(List.of(Arrays.asList(0, null), Arrays.asList(1, null))));
        }
        try (var load = runFiltered("load", body, filter)) {
            assertColumnNames(load.columns(), List.of("id1", "f"));
            assertColumnTypes(load.columns(), List.of("integer", "keyword"));
            assertThat(getValuesList(load), equalTo(List.of(Arrays.asList(0, "7"), Arrays.asList(1, "8"))));
        }
    }

    /**
     * LOAD makes the extra field_caps call while DEFAULT and NULLIFY do not. labels.env is a real keyword in test1, but its parent
     * labels is a flattened field in test2, so labels.env is mapped only in test1. Only LOAD tracks partially-unmapped fields.
     * DEFAULT and NULLIFY never run that check and resolve labels.env from test1, so they succeed on the first pass.
     *
     * field_caps:
     * LOAD            throws first-pass (flattened subfield) → makes the second, unfiltered call, which re-throws the same error - the
     *                 retry is futile since it only adds indices back and the flattened parent is still present;
     * DEFAULT/NULLIFY resolve on the first pass and make a single call.
     */
    public void testLoadFlattenedSubfieldRetriesAndErrorsWhileDefaultNullifySucceed() {
        assumeTrue("requires nullify + load", nullifyEnabled() && loadEnabled());
        createIndex(
            "test1",
            "{ \"properties\": { \"k\": { \"type\": \"integer\" }, "
                + "\"labels\": { \"type\": \"object\", \"properties\": { \"env\": { \"type\": \"keyword\" } } } } }"
        );
        createIndex("test2", "{ \"properties\": { \"k\": { \"type\": \"integer\" }, \"labels\": { \"type\": \"flattened\" } } }");
        indexJson("test1", "a", "{ \"k\": 1, \"labels\": { \"env\": \"prod\" } }");
        indexJson("test2", "b", "{ \"k\": 2, \"labels\": { \"env\": \"dev\", \"other\": \"x\" } }");
        refresh("test1", "test2");

        String body = "FROM test1,test2 | KEEP k, labels.env | SORT k";
        QueryBuilder filter = QueryBuilders.existsQuery("k"); // present so the retry path is active; matches both indices, prunes nothing

        List<List<Object>> expectedValues = List.of(Arrays.asList(1, "prod"), Arrays.asList(2, null));

        try (var def = runFiltered(null, body, filter)) {
            assertThat(def.isPartial(), equalTo(false));
            assertColumnNames(def.columns(), List.of("k", "labels.env"));
            assertColumnTypes(def.columns(), List.of("integer", "keyword"));
            assertThat(getValuesList(def), equalTo(expectedValues));
        }
        try (var nullify = runFiltered("nullify", body, filter)) {
            assertColumnNames(nullify.columns(), List.of("k", "labels.env"));
            assertColumnTypes(nullify.columns(), List.of("integer", "keyword"));
            assertThat(getValuesList(nullify), equalTo(expectedValues));
        }

        Exception e = expectThrows(Exception.class, () -> runFiltered("load", body, filter).close());
        assertThat(
            e.getMessage(),
            containsString("Loading subfield [labels.env] when parent [labels] is of flattened field type is not supported")
        );
    }

    private static boolean nullifyEnabled() {
        return EsqlCapabilities.Cap.OPTIONAL_FIELDS_NULLIFY_TECH_PREVIEW.isEnabled();
    }

    private static boolean loadEnabled() {
        return EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5.isEnabled();
    }
}
