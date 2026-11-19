/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.BranchPlan;
import org.elasticsearch.xpack.esql.plan.logical.ChangePoint;
import org.elasticsearch.xpack.esql.plan.logical.CompoundOutputEval;
import org.elasticsearch.xpack.esql.plan.logical.Dedup;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.Drop;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.FillNull;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.Grok;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LimitBy;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Rename;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.Sample;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.join.AbstractSubqueryJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiPredicate;

import static org.elasticsearch.xpack.esql.CsvTestUtils.isEnabled;
import static org.elasticsearch.xpack.esql.CsvTestUtils.loadCsvSpecValues;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.hamcrest.Matchers.equalTo;

/**
 * Differential test over the {@link CsvIT} csv-spec corpus: every query that passes its csv-spec
 * assertions is rewritten into equivalent forms, and each form must produce the same response as
 * the original.
 */
public class CsvDifferentialIT extends CsvIT {

    private static final List<QueryVariant> VARIANTS = List.of(
        new ViewVariant(null),
        new ViewVariant("unmapped_fields=\"NULLIFY\""),
        new ViewVariant("unmapped_fields=\"LOAD\""),
        new ViewVariant("unmapped_fields=\"LOAD_ALL\"")
    );

    private final List<QueryVariant> variants;

    public CsvDifferentialIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions,
        List<QueryVariant> variants
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions);
        this.variants = variants;
    }

    /**
     * Pairs each enabled csv-spec test with the {@link QueryVariant}s that apply to it, keeping only the tests
     * at least one applies to. Queries are parsed here because parsing on the test thread can add deprecation
     * warnings to its responses.
     */
    @ParametersFactory(argumentFormatting = "csv-spec:%2$s.%3$s", shuffle = false)
    public static List<Object[]> readScriptSpec() throws Exception {
        List<Object[]> generated = new ArrayList<>();
        for (Object[] row : CsvIT.readScriptSpec()) {
            if (isEnabled((String) row[2], (String) row[5], Version.CURRENT) == false) {
                continue;
            }
            CsvTestCase testCase = (CsvTestCase) row[4];
            EsqlStatement statement = TEST_PARSER.unvalidatedStatement(testCase.query, new QueryParams());
            List<QueryVariant> applicable = VARIANTS.stream().filter(v -> v.appliesTo(testCase, statement)).toList();
            if (applicable.isEmpty() == false) {
                Object[] withVariants = Arrays.copyOf(row, row.length + 1);
                withVariants[row.length] = applicable;
                generated.add(withVariants);
            }
        }
        return generated;
    }

    @Override
    protected void afterQueryPassed(String query, Settings pragmas, QueryResult baseline) {
        BiPredicate<Integer, Integer> nondeterministic = nondeterministicCells(testCase);
        AssertionError failure = null;
        for (QueryVariant variant : variants) {
            try {
                assertSameResult(baseline, variant.run(query, pragmas, this), nondeterministic, testCase.ignoreOrder);
            } catch (AssertionError e) {
                AssertionError differs = new AssertionError(
                    "[" + variant.name() + "] differs from the original query: " + e.getMessage(),
                    e
                );
                if (failure == null) {
                    failure = differs;
                } else {
                    failure.addSuppressed(differs);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * Returns the cells, as {@code (row, column)}, the csv-spec does not pin to an exact value. When the
     * row order is ignored, a cell that is not pinned in one row leaves its whole column unpinned.
     */
    private static BiPredicate<Integer, Integer> nondeterministicCells(CsvTestCase testCase) {
        List<List<Object>> expected = loadCsvSpecValues(testCase.expectedResults).values();
        if (testCase.ignoreOrder) {
            Set<Integer> columns = new HashSet<>();
            for (List<Object> row : expected) {
                for (int column = 0; column < row.size(); column++) {
                    if (isNondeterministic(row.get(column))) {
                        columns.add(column);
                    }
                }
            }
            return (row, column) -> columns.contains(column);
        }
        return (row, column) -> row < expected.size()
            && column < expected.get(row).size()
            && isNondeterministic(expected.get(row).get(column));
    }

    private static boolean isNondeterministic(Object expected) {
        return CsvTestUtils.ANY.equals(expected)
            || expected instanceof CsvTestUtils.Range
            || (expected instanceof List<?> values && values.stream().anyMatch(CsvDifferentialIT::isNondeterministic));
    }

    private void assertSameResult(
        QueryResult expected,
        QueryResult actual,
        BiPredicate<Integer, Integer> nondeterministic,
        boolean ignoreOrder
    ) {
        assertThat("column names", actual.columnNames(), equalTo(expected.columnNames()));
        assertThat("column types", actual.dataTypes(), equalTo(expected.dataTypes()));
        List<List<Object>> expectedRows = comparableRows(expected.values(), nondeterministic, ignoreOrder);
        List<List<Object>> actualRows = comparableRows(actual.values(), nondeterministic, ignoreOrder);
        if (expectedRows.equals(actualRows) == false) {
            fail(describeDifference(expectedRows, actualRows));
        }
        assertThat("warnings", new TreeSet<>(actual.warnings()), equalTo(new TreeSet<>(expected.warnings())));
        if (testCase.expectedDocumentsFound != null
            && isNondeterministic(CsvTestUtils.Type.LONG.convert(testCase.expectedDocumentsFound)) == false) {
            assertThat("documents_found", actual.documentsFound(), equalTo(expected.documentsFound()));
        }
        assertThat("approximation_applied", actual.approximationApplied(), equalTo(expected.approximationApplied()));
    }

    private static List<List<Object>> comparableRows(
        List<List<Object>> rows,
        BiPredicate<Integer, Integer> nondeterministic,
        boolean ignoreOrder
    ) {
        List<List<Object>> masked = new ArrayList<>(rows.size());
        for (int row = 0; row < rows.size(); row++) {
            List<Object> copy = new ArrayList<>(rows.get(row));
            for (int column = 0; column < copy.size(); column++) {
                if (nondeterministic.test(row, column)) {
                    copy.set(column, CsvTestUtils.ANY);
                }
            }
            masked.add(copy);
        }
        return ignoreOrder ? masked.stream().sorted(Comparator.comparing(Object::toString)).toList() : masked;
    }

    private static String describeDifference(List<List<Object>> expected, List<List<Object>> actual) {
        for (int i = 0; i < Math.min(expected.size(), actual.size()); i++) {
            if (Objects.equals(expected.get(i), actual.get(i)) == false) {
                return String.format(Locale.ROOT, "row [%d] differs, expected %s but was %s", i, expected.get(i), actual.get(i));
            }
        }
        return String.format(Locale.ROOT, "expected [%d] rows but got [%d]", expected.size(), actual.size());
    }

    /**
     * A rewrite of a csv-spec query into a form that must return the same response.
     */
    interface QueryVariant {
        String name();

        /**
         * Whether the rewrite is expected to return the same response as {@code query}.
         */
        boolean appliesTo(CsvTestCase testCase, EsqlStatement query);

        /**
         * Runs the rewritten query; its warnings use the original query's positions.
         */
        QueryResult run(String query, Settings pragmas, CsvIT test);
    }

    /**
     * Stores the query as a view and reads it with {@code FROM <view>}, optionally after a {@code SET} of a setting
     * scoped at view boundaries, which must not reach the view body.
     */
    static final class ViewVariant implements QueryVariant {
        private static final String VIEW_NAME = "csv_differential_view";
        private static final Set<String> SETTINGS_ALLOWED_IN_VIEWS = Set.of(
            QuerySettings.UNMAPPED_FIELDS.name(),
            QuerySettings.WILDCARDS_MATCH_VIEWS.name(),
            QuerySettings.WILDCARDS_MATCH_DATASETS.name()
        );
        private static final List<String> VIEW_REFERENCES = List.of(
            " (in view [" + VIEW_NAME + "])",
            " nested in view [" + VIEW_NAME + "]",
            " in view [" + VIEW_NAME + "]"
        );

        private final String callerScopedSetting;
        private final boolean callerLoadsAll;

        ViewVariant(@Nullable String callerScopedSetting) {
            this.callerScopedSetting = callerScopedSetting;
            this.callerLoadsAll = callerScopedSetting != null && callerScopedSetting.contains("LOAD_ALL");
        }

        @Override
        public String name() {
            return callerScopedSetting == null ? "view" : "view with SET " + callerScopedSetting;
        }

        @Override
        public boolean appliesTo(CsvTestCase testCase, EsqlStatement query) {
            return readsOnlyIndicesAndRows(query.plan())
                && aggregatesTimeSeriesIfRead(query.plan())
                && (callerLoadsAll == false || query.plan().anyMatch(plan -> supportedInLoadAllMode(plan) == false) == false)
                && testCase.requestTimeRangeGte == null
                && query.settings().stream().allMatch(s -> SETTINGS_ALLOWED_IN_VIEWS.contains(s.name()))
                && query.setting(QuerySettings.UNMAPPED_FIELDS) != UnmappedResolution.LOAD_ALL;
        }

        /**
         * Mirrors {@code Verifier#supportedInLoadAllMode} on the parsed plan: {@code LOAD_ALL} rejects a view body that
         * uses any other command, although the setting does not reach into the view.
         */
        private static boolean supportedInLoadAllMode(LogicalPlan plan) {
            return (plan instanceof UnresolvedRelation relation && relation.indexMode().isTsdb() == false)
                || plan instanceof Project
                || plan instanceof Keep
                || plan instanceof Drop
                || plan instanceof Rename
                || plan instanceof Eval
                || plan instanceof FillNull
                || plan instanceof Grok
                || plan instanceof Dissect
                || plan instanceof MvExpand
                || plan instanceof Sample
                || plan instanceof ChangePoint
                || plan instanceof CompoundOutputEval
                || plan instanceof LimitBy
                || plan instanceof Dedup
                || plan instanceof Filter
                || plan instanceof OrderBy
                || plan instanceof Limit
                || plan instanceof Aggregate
                || plan instanceof InlineStats
                || plan instanceof LookupJoin
                || plan instanceof Enrich
                || plan instanceof Fork
                || plan instanceof BranchPlan
                || plan instanceof UnionAll
                || plan instanceof Subquery
                || plan instanceof AbstractSubqueryJoin;
        }

        private static boolean readsOnlyIndicesAndRows(LogicalPlan plan) {
            return plan.collectLeaves().stream().allMatch(leaf -> leaf instanceof UnresolvedRelation || leaf instanceof Row);
        }

        private static boolean aggregatesTimeSeriesIfRead(LogicalPlan plan) {
            return plan.anyMatch(p -> p instanceof UnresolvedRelation r && r.indexMode() == IndexMode.TIME_SERIES) == false
                || plan.anyMatch(p -> p instanceof TimeSeriesAggregate || p instanceof PromqlCommand);
        }

        @Override
        public QueryResult run(String query, Settings pragmas, CsvIT test) {
            String caller = (callerScopedSetting == null ? "" : "SET " + callerScopedSetting + ";\r\n") + "FROM " + VIEW_NAME;
            test.withoutResponseHeaders(() -> putView(VIEW_NAME, query));
            try {
                return test.runQuery(caller, pragmas).mapWarnings(ViewVariant::removeViewReferences);
            } finally {
                test.withoutResponseHeaders(() -> deleteView(VIEW_NAME));
            }
        }

        private static String removeViewReferences(String warning) {
            for (String reference : VIEW_REFERENCES) {
                warning = warning.replace(reference, "");
            }
            return warning;
        }
    }
}
