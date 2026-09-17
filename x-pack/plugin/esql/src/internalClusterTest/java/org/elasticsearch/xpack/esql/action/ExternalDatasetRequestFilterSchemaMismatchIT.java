/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase.SuiteScopeTestCase;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

/**
 * The request filter over a dataset whose declared column type disagrees with the index mapping over the same values.
 *
 * <p>The differential the other request-filter suites use — the dataset must select what the index selects — does not
 * apply here, because the two sources genuinely hold different types: {@code "10"} as a keyword on one side and
 * {@code 10} as a long on the other answer a range differently, and both answers are right for the type they hold.
 * What must still hold is that the dataset answers its own schema consistently, so the oracle here is the dataset
 * against itself: a filter pushed down must select the rows the equivalent {@code WHERE} selects when the same query
 * runs with no filter at all. A translation that reads the literal against the wrong type diverges from that.
 *
 * <p>The second half of the suite sends literals that cannot be read as the declared type at all. Those must not fail
 * the query: a clause is dropped or folds to no match, and the loosen-only invariant holds — the answer contains every
 * row the equivalent {@code WHERE} selects.
 */
@SuiteScopeTestCase
public class ExternalDatasetRequestFilterSchemaMismatchIT extends AbstractExternalDataSourceIT {

    private static final int ROWS = 40;
    private static final String INDEX = "mismatch_idx";
    private static final String DATASET = "mismatch_ds";

    /**
     * One column of the fixture: the same text in the CSV and in the index, declared as one type on the dataset and
     * mapped as another on the index.
     */
    private record Mismatched(String name, String declaredOnDataset, String mappedOnIndex) {}

    private static final List<Mismatched> COLUMNS = List.of(
        // Digits. The dataset reads them as a number, the index as a keyword: a range means magnitude on one side and
        // lexicographic order on the other, so "9" vs "10" is where they part.
        new Mismatched("digits", "integer", "keyword"),
        // The same values the other way round, so neither direction rests on the dataset being the numeric one.
        new Mismatched("numeric", "keyword", "long"),
        // A timestamp the dataset parses and the index stores as text.
        new Mismatched("stamp", "date", "keyword")
    );

    /** Every third row leaves each column empty, so a filter meets rows that lack the field on both sources. */
    private static boolean present(int row) {
        return row % 3 != 0;
    }

    private static String digits(int row) {
        return String.valueOf(row * 5);
    }

    private static String stamp(int row) {
        return "2020-01-" + Strings.format("%02d", 1 + (row % 28)) + "T00:00:00Z";
    }

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    @Override
    protected void setupSuiteScopeCluster() throws Exception {
        List<String> mapping = new ArrayList<>(List.of("id", "type=integer"));
        LinkedHashMap<String, DatasetFieldMapping> declared = new LinkedHashMap<>();
        declared.put("id", new DatasetFieldMapping("integer", null));
        StringBuilder csv = new StringBuilder("id:integer");
        for (Mismatched column : COLUMNS) {
            mapping.add(column.name());
            mapping.add("type=" + column.mappedOnIndex());
            declared.put(column.name(), new DatasetFieldMapping(column.declaredOnDataset(), null));
            csv.append(',').append(column.name()).append(':').append(column.declaredOnDataset());
        }
        csv.append('\n');

        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(INDEX)
                .setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping(mapping.toArray(String[]::new))
        );
        for (int row = 0; row < ROWS; row++) {
            Map<String, Object> source = new HashMap<>();
            source.put("id", row);
            csv.append(row);
            for (Mismatched column : COLUMNS) {
                String value = switch (column.name()) {
                    case "stamp" -> stamp(row);
                    default -> digits(row);
                };
                csv.append(',');
                if (present(row)) {
                    source.put(column.name(), value);
                    csv.append(value);
                }
            }
            csv.append('\n');
            client().prepareIndex(INDEX).setSource(source).get();
        }
        client().admin().indices().prepareRefresh(INDEX).get();

        Path csvFile = createTempDir().resolve("mismatch.csv");
        Files.writeString(csvFile, csv.toString(), StandardCharsets.UTF_8);
        registerStrictDataset(DATASET, StoragePath.fileUri(csvFile), declared, Map.of("format", "csv", "null_value", ""));
    }

    // ---- positive control ----

    /**
     * The fixture is only a mismatch fixture if the two sources really do read the same text differently. A keyword
     * range over the digits is lexicographic and a numeric range is not, so the two must select different rows here —
     * otherwise every assertion below would hold on a fixture that had no mismatch in it.
     */
    public void testTheTwoSourcesReallyDisagreeOnTheSameValues() {
        QueryBuilder wideRange = QueryBuilders.rangeQuery("digits").gte(10).lte(100);
        List<Object> onIndex = idsMatchingFilter(INDEX, wideRange);
        List<Object> onDataset = idsMatchingFilter(DATASET, wideRange);
        assertFalse("digits must be present on some rows and absent on others", onDataset.isEmpty());
        assertNotEquals(
            "the index reads digits as keyword and the dataset as integer, so a numeric range must not agree",
            onIndex,
            onDataset
        );
    }

    // ---- a pushed-down filter agrees with the same predicate written as WHERE ----

    public void testNumberOnANumericColumnTheIndexCallsKeyword() {
        assertPushdownMatchesWhere(QueryBuilders.termQuery("digits", 50), "digits == 50");
        assertPushdownMatchesWhere(QueryBuilders.rangeQuery("digits").gte(10).lte(100), "digits >= 10 AND digits <= 100");
        assertPushdownMatchesWhere(QueryBuilders.rangeQuery("digits").gt(100), "digits > 100");
    }

    public void testTextOnAKeywordColumnTheIndexCallsLong() {
        assertPushdownMatchesWhere(QueryBuilders.termQuery("numeric", "50"), "numeric == \"50\"");
        assertPushdownMatchesWhere(QueryBuilders.rangeQuery("numeric").gte("10").lte("100"), "numeric >= \"10\" AND numeric <= \"100\"");
    }

    public void testTimestampOnADateColumnTheIndexCallsKeyword() {
        assertPushdownMatchesWhere(
            QueryBuilders.rangeQuery("stamp").gte("2020-01-05T00:00:00Z").lte("2020-01-20T00:00:00Z"),
            "stamp >= \"2020-01-05T00:00:00Z\"::datetime AND stamp <= \"2020-01-20T00:00:00Z\"::datetime"
        );
    }

    public void testExistsAgreesOnEveryMismatchedColumn() {
        for (Mismatched column : COLUMNS) {
            assertPushdownMatchesWhere(QueryBuilders.existsQuery(column.name()), column.name() + " IS NOT NULL");
        }
    }

    public void testNegationAgreesOnEveryMismatchedColumn() {
        // A missing field is the case a negation gets wrong most easily: NOT(null) must not return the row.
        for (Mismatched column : COLUMNS) {
            assertPushdownMatchesWhere(
                QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(column.name())),
                "NOT (" + column.name() + " IS NOT NULL)"
            );
        }
    }

    // ---- a literal that cannot be read as the declared type ----

    /**
     * Each of these is a literal the dataset's declared type cannot hold. None may fail the query, and each must return
     * at least the rows the equivalent predicate selects — the clause is dropped, or folds to no match, never to a
     * narrower answer than the predicate it stands for.
     */
    public void testUnreadableLiteralsNeverFailTheQuery() {
        List<String> failures = new ArrayList<>();
        assertLooseOnly(QueryBuilders.termQuery("digits", "not-a-number"), "false", failures);
        assertLooseOnly(QueryBuilders.rangeQuery("digits").gte("not-a-number"), "false", failures);
        assertLooseOnly(QueryBuilders.termQuery("stamp", "not-a-date"), "false", failures);
        assertLooseOnly(QueryBuilders.rangeQuery("stamp").gte("2020-13-45T99:99:99Z"), "false", failures);
        // A number against a keyword column: readable as text, so this one may legitimately select rows.
        assertLooseOnly(QueryBuilders.termQuery("numeric", 50), "numeric == \"50\"", failures);
        if (failures.isEmpty() == false) {
            fail(failures.size() + " unreadable literal(s) misbehaved:\n" + String.join("\n", failures));
        }
    }

    /** The index rejects some of these outright. Whatever the index does, the dataset must still answer. */
    public void testTheIndexRejectingAFilterDoesNotStopTheDataset() {
        List<QueryBuilder> filters = List.of(
            QueryBuilders.termQuery("numeric", "not-a-number"),
            QueryBuilders.rangeQuery("numeric").gte("not-a-number"),
            QueryBuilders.termQuery("digits", "2020-01-01T00:00:00Z")
        );
        for (QueryBuilder filter : filters) {
            try {
                idsMatchingFilter(INDEX, filter);
            } catch (Exception indexRejects) {
                // Expected for a literal the index's own mapping cannot read; the dataset is what this asserts about.
            }
            idsMatchingFilter(DATASET, filter);
        }
    }

    // ---- the oracle ----

    /**
     * The dataset answers the pushed-down filter exactly as it answers the same predicate written as {@code WHERE},
     * which is the dataset's own reading of its own schema.
     */
    private void assertPushdownMatchesWhere(QueryBuilder filter, String where) {
        List<Object> pushedDown = idsMatchingFilter(DATASET, filter);
        List<Object> written = idsMatchingWhere(DATASET, where);
        assertEquals("pushed down " + Strings.toString(filter) + " disagrees with WHERE " + where, written, pushedDown);
    }

    private void assertLooseOnly(QueryBuilder filter, String where, List<String> failures) {
        String described = Strings.toString(filter);
        List<Object> pushedDown;
        try {
            pushedDown = idsMatchingFilter(DATASET, filter);
        } catch (Exception e) {
            failures.add(described + " — failed the query: " + e);
            return;
        }
        List<Object> written = idsMatchingWhere(DATASET, where);
        if (pushedDown.containsAll(written) == false) {
            failures.add(described + " — returned fewer rows than WHERE " + where + ". filter " + pushedDown + ", where " + written);
        }
    }

    private List<Object> idsMatchingFilter(String source, QueryBuilder filter) {
        return ids(syncEsqlQueryRequest("FROM " + source + " | KEEP id | SORT id").filter(filter));
    }

    private List<Object> idsMatchingWhere(String source, String where) {
        return ids(syncEsqlQueryRequest("FROM " + source + " | WHERE " + where + " | KEEP id | SORT id"));
    }

    private List<Object> ids(EsqlQueryRequest request) {
        try (EsqlQueryResponse response = run(request)) {
            return getValuesList(response).stream().map(row -> row.get(0)).toList();
        }
    }
}
