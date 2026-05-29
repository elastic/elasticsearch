/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.http.HttpDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.dataset.DeleteDatasetAction;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.datasource.DeleteDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.PutDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * End-to-end integration for subqueries in the FROM clause whose source is a registered dataset
 * (i.e. {@code FROM ..., (FROM <dataset> | ...)}). Builds on {@link FromDatasetIT}: same CRUD-driven
 * setup, same CSV fixture shape — only difference is the subquery wrapper around the dataset
 * reference. Pins the contract that {@code DatasetRewriter} runs per {@code UnresolvedRelation}
 * (each FROM, top-level or in a subquery, is rewritten independently) and that the resulting
 * {@code UnresolvedExternalRelation} composes correctly with the {@code UnionAll} the
 * subquery-in-FROM machinery emits.
 *
 * <p>Single-node by design, mirroring {@link FromDatasetIT}: multi-node dataset publication trips
 * an unrelated {@code ProjectMetadata.Builder} assertion on {@code main}.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class FromDatasetSubqueryIT extends AbstractEsqlIntegTestCase {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);
    private Path csvFixture;
    private Path csvFixtureAlt;

    /** Minimal pass-through validator registered for type {@code test}; accepts any resource scheme. */
    public static final class TestDataSourcePlugin extends Plugin implements DataSourcePlugin {
        @Override
        public Map<String, DataSourceValidator> datasourceValidators(Settings settings) {
            return Map.of("test", new TestValidator());
        }
    }

    private static final class TestValidator implements DataSourceValidator {
        @Override
        public String type() {
            return "test";
        }

        @Override
        public Map<String, DataSourceSetting> validateDatasource(Map<String, Object> datasourceSettings) {
            Map<String, DataSourceSetting> out = new HashMap<>();
            for (Map.Entry<String, Object> e : datasourceSettings.entrySet()) {
                out.put(e.getKey(), new DataSourceSetting(e.getValue(), e.getKey().startsWith("secret_")));
            }
            return out;
        }

        @Override
        public Map<String, Object> validateDataset(
            Map<String, DataSourceSetting> datasourceSettings,
            String resource,
            Map<String, Object> datasetSettings
        ) {
            return datasetSettings == null ? Map.of() : new HashMap<>(datasetSettings);
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(HttpDataSourcePlugin.class);
        plugins.add(CsvDataSourcePlugin.class);
        plugins.add(TestDataSourcePlugin.class);
        return plugins;
    }

    /** Determinism over planner-regression diversity here — these tests pin specific plan shapes. */
    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    @Before
    public void requireFeatureFlag() {
        assumeTrue("requires external data source subquery support", EsqlCapabilities.Cap.SUBQUERY_WITH_EXTERNAL_DATA_SOURCE.isEnabled());
    }

    @Before
    public void writeFixture() throws IOException {
        // Five-column schema gives downstream tests something to group by (department), aggregate
        // (salary), and join on (department → departments_lookup.location). Mirrors the shape of the
        // existing employees fixture used by subquery.csv-spec where useful, while staying minimal.
        csvFixture = createTempFile("dataset-fixture-", ".csv");
        Files.writeString(
            csvFixture,
            String.join(
                "\n",
                "emp_no:integer,first_name:keyword,last_name:keyword,department:keyword,salary:integer",
                "1,Alice,Anderson,Engineering,50000",
                "2,Bob,Brown,Engineering,60000",
                "3,Carol,Cox,Sales,55000"
            ) + "\n"
        );
        csvFixtureAlt = createTempFile("dataset-fixture-alt-", ".csv");
        Files.writeString(
            csvFixtureAlt,
            String.join(
                "\n",
                "emp_no:integer,first_name:keyword,last_name:keyword,department:keyword,salary:integer",
                "10,Diana,Davis,Engineering,75000",
                "11,Eve,Evans,Sales,65000"
            ) + "\n"
        );
    }

    /**
     * Names every dataset {@code testXxx} bodies PUT. New tests must register their dataset name
     * here so the SUITE-scoped cluster doesn't carry state across methods.
     */
    private static final Set<String> CREATED_DATASETS = Set.of("employees", "employees_alt");

    @After
    public void cleanupRegistry() throws Exception {
        for (String ds : CREATED_DATASETS) {
            try {
                client().execute(DeleteDatasetAction.INSTANCE, deleteDatasetRequest(ds)).get(30, java.util.concurrent.TimeUnit.SECONDS);
            } catch (ResourceNotFoundException ignored) {
                // already deleted by the test itself
            } catch (Exception e) {
                logger.warn("dataset cleanup [{}] failed", ds, e);
            }
        }
        try {
            client().execute(DeleteDataSourceAction.INSTANCE, deleteDataSourceRequest("local_ds"))
                .get(30, java.util.concurrent.TimeUnit.SECONDS);
        } catch (ResourceNotFoundException ignored) {
            // already deleted by the test itself
        } catch (Exception e) {
            logger.warn("data source cleanup [local_ds] failed", e);
        }
    }

    /**
     * Bare subquery whose only source is a dataset: {@code FROM (FROM <dataset>)}. The rewriter
     * visits the inner {@code UnresolvedRelation}, swaps it for {@code UnresolvedExternalRelation},
     * and the outer FROM collapses to that single child — equivalent to {@code FROM <dataset>}.
     */
    public void testSubqueryOnlyDataset() throws Exception {
        registerEmployees();

        try (var response = run(syncEsqlQueryRequest("FROM (FROM employees) | SORT emp_no"), TIMEOUT)) {
            List<? extends ColumnInfo> columns = response.columns();
            // Five columns from the fixture header — emp_no, first_name, last_name, department, salary.
            assertThat(columns, hasSize(5));
            assertThat(columns.get(0).name(), equalTo("emp_no"));
            assertThat(columns.get(1).name(), equalTo("first_name"));

            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            assertThat(rows.get(0).get(0), equalTo(1));
            assertThat(rows.get(0).get(1).toString(), equalTo("Alice"));
            assertThat(rows.get(2).get(0), equalTo(3));
        }
    }

    /**
     * Main FROM and subquery both target datasets: {@code FROM <ds1>, (FROM <ds2>)}. Both legs are
     * rewritten to {@code UnresolvedExternalRelation} and combined through the subquery-in-FROM
     * {@code UnionAll}. Counts the union — same expected total as {@code FROM <ds1>, <ds2>} in
     * {@link FromDatasetIT#testFromMultipleDatasets}.
     */
    public void testMainDatasetWithSubqueryDataset() throws Exception {
        registerEmployees();
        registerEmployeesAlt();

        try (var response = run(syncEsqlQueryRequest("FROM employees, (FROM employees_alt) | STATS c = COUNT(*)"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            // 3 from employees + 2 from employees_alt
            assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(5L));
        }
    }

    /** No main index pattern — only multiple dataset subqueries: {@code FROM (FROM ds1), (FROM ds2)}. */
    public void testMultipleSubqueriesOnDatasets() throws Exception {
        registerEmployees();
        registerEmployeesAlt();

        try (var response = run(syncEsqlQueryRequest("FROM (FROM employees), (FROM employees_alt) | STATS c = COUNT(*)"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(5L));
        }
    }

    /**
     * Index in the main FROM and dataset in the subquery: {@code FROM <index>, (FROM <dataset>)}.
     * The mixed-FROM rejection fires <em>per</em> {@code UnresolvedRelation} (it checks one FROM's
     * target list, not across FROM boundaries), so each leg is uniform — index outside, dataset
     * inside — and the query is accepted. Regression guard against a future overreach that would
     * apply the rejection plan-wide.
     */
    public void testIndexInMainDatasetInSubqueryAccepted() throws Exception {
        // Two-doc real index lets us assert the union arithmetic instead of just non-failure.
        createIndex("real_employees");
        ensureGreen("real_employees");
        client().prepareIndex("real_employees").setSource("emp_no", 100, "first_name", "Frank").get();
        client().prepareIndex("real_employees").setSource("emp_no", 101, "first_name", "Grace").get();
        client().admin().indices().prepareRefresh("real_employees").get();

        registerEmployees();

        try (var response = run(syncEsqlQueryRequest("FROM real_employees, (FROM employees) | STATS c = COUNT(*)"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            // 2 from real_employees + 3 from employees
            assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(5L));
        }
    }

    /** Subquery applies a filter before union: {@code FROM (FROM <dataset> | WHERE emp_no > 1)}. */
    public void testSubqueryWithFilter() throws Exception {
        registerEmployees();

        try (var response = run(syncEsqlQueryRequest("FROM (FROM employees | WHERE emp_no > 1) | SORT emp_no"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(2));
            assertThat(rows.get(0).get(0), equalTo(2));
            assertThat(rows.get(0).get(1).toString(), equalTo("Bob"));
            assertThat(rows.get(1).get(0), equalTo(3));
            assertThat(rows.get(1).get(1).toString(), equalTo("Carol"));
        }
    }

    /** Subquery narrows the column set before union: {@code FROM (FROM <dataset> | KEEP first_name)}. */
    public void testSubqueryWithKeep() throws Exception {
        registerEmployees();

        try (var response = run(syncEsqlQueryRequest("FROM (FROM employees | KEEP first_name) | SORT first_name"), TIMEOUT)) {
            List<? extends ColumnInfo> columns = response.columns();
            assertThat(columns, hasSize(1));
            assertThat(columns.get(0).name(), equalTo("first_name"));

            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            assertThat(rows.get(0).get(0).toString(), equalTo("Alice"));
            assertThat(rows.get(1).get(0).toString(), equalTo("Bob"));
            assertThat(rows.get(2).get(0).toString(), equalTo("Carol"));
        }
    }

    /** Aggregation inside the subquery: {@code FROM (FROM <dataset> | STATS c = COUNT(*))}. */
    public void testSubqueryWithStats() throws Exception {
        registerEmployees();

        try (var response = run(syncEsqlQueryRequest("FROM (FROM employees | STATS c = COUNT(*))"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(3L));
        }
    }

    /** Computed column inside the subquery: {@code FROM (FROM <dataset> | EVAL doubled = emp_no * 2)}. */
    public void testSubqueryWithEval() throws Exception {
        registerEmployees();

        try (
            var response = run(syncEsqlQueryRequest("FROM (FROM employees | EVAL doubled = emp_no * 2) | SORT emp_no | LIMIT 1"), TIMEOUT)
        ) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            int empNoIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("emp_no");
            int doubledIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("doubled");
            assertThat(rows.get(0).get(empNoIdx), equalTo(1));
            assertThat(((Number) rows.get(0).get(doubledIdx)).intValue(), equalTo(2));
        }
    }

    /**
     * Mixed targets <em>within</em> the subquery's FROM are still rejected — the per-FROM rejection
     * from {@link FromDatasetIT#testFromMixedIndexAndDatasetRejected} also fires for a FROM living
     * inside a subquery (the rewriter doesn't care which subtree position the {@code UnresolvedRelation}
     * occupies).
     */
    public void testMixedTargetsInSubqueryRejected() throws Exception {
        createIndex("real_employees");
        ensureGreen("real_employees");
        registerEmployees();

        Exception ex = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM (FROM real_employees, employees) | LIMIT 1"), TIMEOUT)
        );
        assertCauseMessageContains(ex, "mixing datasets and non-datasets");
    }

    /**
     * METADATA fields on a dataset are rejected even when the dataset reference is inside a
     * subquery — same code path as {@link FromDatasetIT#testFromDatasetWithMetadataFieldsRejected},
     * confirming the rewriter's METADATA check is position-agnostic.
     */
    public void testMetadataOnDatasetInSubqueryRejected() throws Exception {
        registerEmployees();

        Exception ex = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM (FROM employees METADATA _index | KEEP _index) | LIMIT 1"), TIMEOUT)
        );
        assertCauseMessageContains(ex, "METADATA fields are not supported on datasets");
        assertCauseMessageContains(ex, "employees");
    }

    // ------------------------------------------------------------------------
    // Richer processing pipelines inside the subquery
    // ------------------------------------------------------------------------

    /**
     * STATS with a grouping key inside the subquery: aggregation runs against the dataset's columns
     * before the subquery's output enters the outer FROM. Confirms the pre-union aggregation plan
     * shape is plumbed through correctly when the source is external.
     */
    public void testSubqueryWithStatsByGrouping() throws Exception {
        registerEmployees();

        try (
            var response = run(
                syncEsqlQueryRequest("FROM (FROM employees | STATS total = SUM(salary), c = COUNT(*) BY department) | SORT department"),
                TIMEOUT
            )
        ) {
            List<List<Object>> rows = getValuesList(response);
            int totalIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("total");
            int cIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("c");
            int deptIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("department");
            assertThat(rows, hasSize(2));
            // Engineering: 50000+60000=110000, count=2 | Sales: 55000, count=1
            assertThat(rows.get(0).get(deptIdx).toString(), equalTo("Engineering"));
            assertThat(((Number) rows.get(0).get(totalIdx)).longValue(), equalTo(110000L));
            assertThat(((Number) rows.get(0).get(cIdx)).longValue(), equalTo(2L));
            assertThat(rows.get(1).get(deptIdx).toString(), equalTo("Sales"));
            assertThat(((Number) rows.get(1).get(totalIdx)).longValue(), equalTo(55000L));
            assertThat(((Number) rows.get(1).get(cIdx)).longValue(), equalTo(1L));
        }
    }

    /** SORT + LIMIT inside the subquery — TopN runs against the dataset rows before they leave the subquery. */
    public void testSubqueryWithSortAndLimit() throws Exception {
        registerEmployees();

        try (var response = run(syncEsqlQueryRequest("FROM (FROM employees | SORT salary DESC | LIMIT 2) | SORT salary DESC"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            int empNoIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("emp_no");
            int salaryIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("salary");
            assertThat(rows, hasSize(2));
            assertThat(rows.get(0).get(empNoIdx), equalTo(2));
            assertThat(((Number) rows.get(0).get(salaryIdx)).intValue(), equalTo(60000));
            assertThat(rows.get(1).get(empNoIdx), equalTo(3));
            assertThat(((Number) rows.get(1).get(salaryIdx)).intValue(), equalTo(55000));
        }
    }

    /** EVAL → WHERE → KEEP chain inside the subquery: multi-step transformation contained in the subquery. */
    public void testSubqueryWithEvalWhereChain() throws Exception {
        registerEmployees();

        try (
            var response = run(
                syncEsqlQueryRequest(
                    "FROM (FROM employees | EVAL bonus = salary * 0.1 | WHERE bonus > 5500 | KEEP emp_no, bonus) | SORT emp_no"
                ),
                TIMEOUT
            )
        ) {
            List<List<Object>> rows = getValuesList(response);
            // bonus > 5500 → salary > 55000 → only Bob (60000)
            assertThat(rows, hasSize(1));
            int empNoIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("emp_no");
            int bonusIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("bonus");
            assertThat(rows.get(0).get(empNoIdx), equalTo(2));
            assertThat(((Number) rows.get(0).get(bonusIdx)).doubleValue(), equalTo(6000.0));
        }
    }

    /** DISSECT inside the subquery — string-parsing command operates on dataset rows pre-union. */
    public void testSubqueryWithDissect() throws Exception {
        registerEmployees();

        try (
            var response = run(
                syncEsqlQueryRequest("FROM (FROM employees | DISSECT first_name \"%{first_letter}lice\" | KEEP emp_no, first_letter)"),
                TIMEOUT
            )
        ) {
            List<List<Object>> rows = getValuesList(response);
            // Only "Alice" matches the %{first_letter}lice pattern (first_letter = "A"); the other rows
            // produce nulls for the dissected column.
            assertThat(rows, hasSize(3));
            int letterIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("first_letter");
            long matches = rows.stream().filter(r -> r.get(letterIdx) != null && r.get(letterIdx).toString().equals("A")).count();
            assertThat(matches, equalTo(1L));
        }
    }

    /**
     * LOOKUP JOIN inside the subquery, joining dataset rows against a real ES lookup index. The JOIN
     * target is the lookup index (not a dataset), so the rewriter's "LOOKUP JOIN against a dataset"
     * rejection does not fire — the dataset only provides the upstream rows.
     */
    public void testSubqueryWithLookupJoin() throws Exception {
        registerEmployees();
        createDepartmentsLookup();

        try (
            var response = run(
                syncEsqlQueryRequest(
                    "FROM (FROM employees | LOOKUP JOIN departments_lookup ON department | KEEP emp_no, department, location) | SORT emp_no"
                ),
                TIMEOUT
            )
        ) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            int locIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("location");
            assertThat(rows.get(0).get(locIdx).toString(), equalTo("Mountain View")); // Alice / Engineering
            assertThat(rows.get(1).get(locIdx).toString(), equalTo("Mountain View")); // Bob / Engineering
            assertThat(rows.get(2).get(locIdx).toString(), equalTo("New York"));      // Carol / Sales
        }
    }

    /** INLINE STATS inside the subquery — the aggregation result is broadcast back to every row before union. */
    public void testSubqueryWithInlineStats() throws Exception {
        registerEmployees();

        try (var response = run(syncEsqlQueryRequest("""
            FROM (FROM employees
                  | INLINE STATS max_salary = MAX(salary)
                  | WHERE salary == max_salary
                  | KEEP emp_no, salary, max_salary)
            """), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            // Bob has the max salary (60000) — INLINE STATS attaches max_salary=60000 to every row,
            // then the WHERE keeps just the row whose salary equals it.
            assertThat(rows, hasSize(1));
            int empNoIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("emp_no");
            int maxIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("max_salary");
            assertThat(rows.get(0).get(empNoIdx), equalTo(2));
            assertThat(((Number) rows.get(0).get(maxIdx)).intValue(), equalTo(60000));
        }
    }

    // ------------------------------------------------------------------------
    // Processing commands after the subqueries
    // ------------------------------------------------------------------------

    /** STATS BY runs over the unioned dataset+dataset output, not inside either subquery. */
    public void testStatsByAfterSubquery() throws Exception {
        registerEmployees();
        registerEmployeesAlt();

        try (
            var response = run(
                syncEsqlQueryRequest(
                    "FROM (FROM employees), (FROM employees_alt) | STATS total = SUM(salary), c = COUNT(*) BY department | SORT department"
                ),
                TIMEOUT
            )
        ) {
            List<List<Object>> rows = getValuesList(response);
            int totalIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("total");
            int cIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("c");
            int deptIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("department");
            assertThat(rows, hasSize(2));
            // Engineering across both: 50000+60000+75000 = 185000, count=3
            // Sales across both: 55000+65000 = 120000, count=2
            assertThat(rows.get(0).get(deptIdx).toString(), equalTo("Engineering"));
            assertThat(((Number) rows.get(0).get(totalIdx)).longValue(), equalTo(185000L));
            assertThat(((Number) rows.get(0).get(cIdx)).longValue(), equalTo(3L));
            assertThat(rows.get(1).get(deptIdx).toString(), equalTo("Sales"));
            assertThat(((Number) rows.get(1).get(totalIdx)).longValue(), equalTo(120000L));
            assertThat(((Number) rows.get(1).get(cIdx)).longValue(), equalTo(2L));
        }
    }

    /** WHERE → STATS pipeline applied to the unioned dataset rows. */
    public void testWhereAndStatsAfterSubquery() throws Exception {
        registerEmployees();
        registerEmployeesAlt();

        try (var response = run(syncEsqlQueryRequest("""
            FROM (FROM employees), (FROM employees_alt)
            | WHERE department == "Engineering"
            | STATS c = COUNT(*), avg_salary = AVG(salary)
            """), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            int cIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("c");
            int avgIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("avg_salary");
            // Engineering rows across both datasets: salaries 50000, 60000, 75000 → avg ≈ 61666.67
            assertThat(((Number) rows.get(0).get(cIdx)).longValue(), equalTo(3L));
            assertThat(((Number) rows.get(0).get(avgIdx)).doubleValue(), equalTo((50000.0 + 60000.0 + 75000.0) / 3.0));
        }
    }

    /**
     * LOOKUP JOIN runs <em>after</em> the subquery. The subquery emits dataset rows, the outer
     * pipeline joins each row against the real ES lookup index. Same plan shape as
     * {@link #testSubqueryWithLookupJoin} from the LOOKUP JOIN operator's point of view; what's
     * pinned here is that JOIN composes with the outer UnionAll output without reaching back into
     * the dataset relation.
     */
    public void testLookupJoinAfterSubquery() throws Exception {
        registerEmployees();
        registerEmployeesAlt();
        createDepartmentsLookup();

        try (
            var response = run(
                syncEsqlQueryRequest(
                    "FROM (FROM employees), (FROM employees_alt) | LOOKUP JOIN departments_lookup ON department"
                        + " | KEEP emp_no, department, location | SORT emp_no"
                ),
                TIMEOUT
            )
        ) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(5));
            int deptIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("department");
            int locIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("location");
            for (List<Object> row : rows) {
                String dept = row.get(deptIdx).toString();
                String loc = row.get(locIdx).toString();
                if (dept.equals("Engineering")) {
                    assertThat(loc, equalTo("Mountain View"));
                } else if (dept.equals("Sales")) {
                    assertThat(loc, equalTo("New York"));
                } else {
                    throw new AssertionError("unexpected department: " + dept);
                }
            }
        }
    }

    /** INLINE STATS after the subquery — broadcast aggregation over the unioned rows. */
    public void testInlineStatsAfterSubquery() throws Exception {
        registerEmployees();
        registerEmployeesAlt();

        try (
            var response = run(
                syncEsqlQueryRequest(
                    "FROM (FROM employees), (FROM employees_alt)"
                        + " | INLINE STATS max_salary = MAX(salary)"
                        + " | WHERE salary == max_salary"
                        + " | KEEP emp_no, salary, max_salary"
                ),
                TIMEOUT
            )
        ) {
            List<List<Object>> rows = getValuesList(response);
            // Across both datasets, Diana (75000) holds the max.
            assertThat(rows, hasSize(1));
            int empNoIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("emp_no");
            int maxIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("max_salary");
            assertThat(rows.get(0).get(empNoIdx), equalTo(10));
            assertThat(((Number) rows.get(0).get(maxIdx)).intValue(), equalTo(75000));
        }
    }

    /** EVAL → SORT → LIMIT after the subquery: computed column drives an outer TopN. */
    public void testEvalSortLimitAfterSubquery() throws Exception {
        registerEmployees();
        registerEmployeesAlt();

        try (var response = run(syncEsqlQueryRequest("""
            FROM (FROM employees), (FROM employees_alt)
            | EVAL bonus = salary * 0.1
            | SORT bonus DESC
            | KEEP emp_no, bonus
            | LIMIT 2
            """), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(2));
            int empNoIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("emp_no");
            int bonusIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("bonus");
            // Top two bonuses: Diana 7500, Eve 6500
            assertThat(rows.get(0).get(empNoIdx), equalTo(10));
            assertThat(((Number) rows.get(0).get(bonusIdx)).doubleValue(), equalTo(7500.0));
            assertThat(rows.get(1).get(empNoIdx), equalTo(11));
            assertThat(((Number) rows.get(1).get(bonusIdx)).doubleValue(), equalTo(6500.0));
        }
    }

    // ------------------------------------------------------------------------
    // Full-text functions against external data sources
    // ------------------------------------------------------------------------
    //
    // Full-text functions need an inverted index, which datasets don't have. AnalyzerExternalTests
    // already pins the rejection messages against the inline EXTERNAL command; these tests confirm
    // the same checks fire when the external source enters via FROM <dataset> inside a subquery —
    // i.e. the verifier looks at the relation type (ExternalRelation), not at the source-command
    // syntax that produced it.

    /**
     * MATCH against a dataset field is rejected: the field comes from {@code ExternalRelation}, not
     * an index mapping, so {@code FullTextFunction#fieldVerifier} fails with the
     * "not a field from an index mapping" message.
     */
    public void testMatchOnDatasetFieldRejected() throws Exception {
        registerEmployees();

        Exception ex = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM (FROM employees | WHERE MATCH(first_name, \"Alice\")) | LIMIT 1"), TIMEOUT)
        );
        assertCauseMessageContains(ex, "MATCH");
        assertCauseMessageContains(ex, "not a field from an index mapping");
    }

    /**
     * MATCH_PHRASE shares the same field-origin check as MATCH; rejection message format is identical.
     */
    public void testMatchPhraseOnDatasetFieldRejected() throws Exception {
        registerEmployees();

        Exception ex = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM (FROM employees | WHERE MATCH_PHRASE(first_name, \"Alice\")) | LIMIT 1"), TIMEOUT)
        );
        assertCauseMessageContains(ex, "MATCH_PHRASE");
        assertCauseMessageContains(ex, "not a field from an index mapping");
    }

    /**
     * KQL and QSTR don't bind to a specific field; they fail the source-command predicate (the
     * relation isn't an {@code EsRelation}) and the verifier emits "cannot be used after &lt;cmd&gt;".
     * Asserting only on the "cannot be used after" prefix keeps the test tolerant of the exact
     * command-name token the verifier extracts from the source text.
     */
    public void testKqlOnDatasetRejected() throws Exception {
        registerEmployees();

        Exception ex = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM (FROM employees | WHERE KQL(\"first_name: Alice\")) | LIMIT 1"), TIMEOUT)
        );
        assertCauseMessageContains(ex, "cannot be used after");
    }

    /** QSTR rejection mirrors KQL — same verifier path, same message shape. */
    public void testQstrOnDatasetRejected() throws Exception {
        registerEmployees();

        Exception ex = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM (FROM employees | WHERE QSTR(\"first_name: Alice\")) | LIMIT 1"), TIMEOUT)
        );
        assertCauseMessageContains(ex, "cannot be used after");
    }

    /**
     * Full-text rejection also fires when the function is applied <em>after</em> the subquery — the
     * outer pipeline's view of the relation is the same {@code ExternalRelation}.
     */
    public void testMatchAfterSubqueryRejected() throws Exception {
        registerEmployees();

        Exception ex = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM (FROM employees) | WHERE MATCH(first_name, \"Alice\") | LIMIT 1"), TIMEOUT)
        );
        assertCauseMessageContains(ex, "MATCH");
        assertCauseMessageContains(ex, "not a field from an index mapping");
    }

    // ------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------

    /**
     * Creates a tiny lookup index that maps each department to a single location. Used by the two
     * LOOKUP JOIN tests; the schema is intentionally trivial — what's being exercised is the join
     * plumbing between a dataset-sourced left side and a regular ES lookup-index right side, not
     * the lookup data itself.
     */
    private void createDepartmentsLookup() {
        Settings lookupSettings = Settings.builder().put("index.number_of_shards", 1).put("index.mode", "lookup").build();
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("departments_lookup")
                .setSettings(lookupSettings)
                .setMapping("department", "type=keyword", "location", "type=keyword")
        );
        client().prepareIndex("departments_lookup").setSource("department", "Engineering", "location", "Mountain View").get();
        client().prepareIndex("departments_lookup").setSource("department", "Sales", "location", "New York").get();
        client().admin().indices().prepareRefresh("departments_lookup").get();
    }

    private void registerEmployees() {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"))
            )
        );
    }

    private void registerEmployeesAlt() {
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest("employees_alt", "local_ds", csvFixtureAlt.toUri().toString(), Map.of("format", "csv"))
            )
        );
    }

    /** Walks the cause chain and asserts a message fragment appears somewhere in it. */
    private static void assertCauseMessageContains(Throwable throwable, String fragment) {
        Throwable cause = throwable;
        while (cause != null && (cause.getMessage() == null || cause.getMessage().contains(fragment) == false)) {
            cause = cause.getCause();
        }
        assertThat("error chain should contain message fragment [" + fragment + "]", cause, org.hamcrest.Matchers.notNullValue());
    }

    private static PutDataSourceAction.Request putDataSourceRequest(String name, Map<String, Object> settings) {
        return new PutDataSourceAction.Request(TIMEOUT, TIMEOUT, name, "test", null, new HashMap<>(settings));
    }

    private static PutDatasetAction.Request putDatasetRequest(
        String name,
        String dataSource,
        String resource,
        Map<String, Object> settings
    ) {
        return new PutDatasetAction.Request(TIMEOUT, TIMEOUT, name, dataSource, resource, null, new HashMap<>(settings));
    }

    private static DeleteDataSourceAction.Request deleteDataSourceRequest(String name) {
        return new DeleteDataSourceAction.Request(TIMEOUT, TIMEOUT, new String[] { name });
    }

    private static DeleteDatasetAction.Request deleteDatasetRequest(String name) {
        return new DeleteDatasetAction.Request(TIMEOUT, TIMEOUT, new String[] { name });
    }
}
