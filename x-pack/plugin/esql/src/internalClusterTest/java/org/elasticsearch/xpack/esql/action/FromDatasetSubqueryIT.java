/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.view.DeleteViewAction;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

/**
 * End-to-end integration for subqueries in the FROM clause whose source is a registered dataset
 * (i.e. {@code FROM ..., (FROM <dataset> | ...)}). Builds on {@link FromDatasetIT}: same CRUD-driven setup, same CSV fixture shape, the
 * only difference is the subquery wrapper around the dataset reference. Pins the contract that {@code DatasetRewriter} runs per
 * {@code UnresolvedRelation} (each FROM, top-level or in a subquery, is rewritten independently) and that the resulting
 * {@code UnresolvedExternalRelation} composes correctly with the {@code UnionAll} the subquery-in-FROM machinery emits.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class FromDatasetSubqueryIT extends AbstractExternalDataSourceIT {

    private Path csvFixture;
    private Path csvFixtureAlt;
    private Path csvFixtureSalaryInt;
    private Path csvFixtureSalaryLong;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    @Before
    public void requireCapability() {
        assumeTrue("requires external dataset in from command support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
    }

    private static void requireInSubquery() {
        assumeTrue("Requires WHERE IN subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITHOUT_VIEW.isEnabled());

    }

    private static void requireWhereInSubqueryWithTS() {
        assumeTrue("Requires IN subquery with TS source support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_TS.isEnabled());
    }

    @Before
    public void writeFixture() throws IOException {
        // Five-column schema gives downstream tests something to group by (department), aggregate (salary), and join on
        // (department → departments_lookup.location). Mirrors the shape of the existing employees fixture used by subquery.csv-spec where
        // useful, while staying minimal.
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
        // Two fixtures that share emp_no:integer and name:keyword but DISAGREE on salary's type (integer vs long). Unioned by name across
        // subquery branches this is a hard type conflict, which the mixed-data-type tests below pin: it only resolves when an explicit
        // cast aligns salary to a single type across every branch.
        csvFixtureSalaryInt = createTempFile("dataset-fixture-salary-int-", ".csv");
        Files.writeString(
            csvFixtureSalaryInt,
            String.join("\n", "emp_no:integer,name:keyword,salary:integer", "1,Alice,50000", "2,Bob,60000") + "\n"
        );
        csvFixtureSalaryLong = createTempFile("dataset-fixture-salary-long-", ".csv");
        Files.writeString(
            csvFixtureSalaryLong,
            String.join("\n", "emp_no:integer,name:keyword,salary:long", "10,Diana,75000", "11,Eve,65000") + "\n"
        );
    }

    /** Names every view {@code testXxx} bodies PUT, dropped after each method so the SUITE cluster stays clean. */
    private static final Set<String> CREATED_VIEWS = Set.of("emp_meta_view");

    /**
     * Datasets and the {@code local_ds} data source are registered through the base
     * {@link AbstractExternalDataSourceIT#registerDataset}/{@link AbstractExternalDataSourceIT#registerDataSource}
     * helpers, so the base {@code cleanupRegistry()} tears them down. Only views need bespoke teardown here.
     */
    @After
    public void cleanupViews() {
        try {
            client().execute(
                DeleteViewAction.INSTANCE,
                new DeleteViewAction.Request(TIMEOUT, TIMEOUT, CREATED_VIEWS.toArray(String[]::new))
            ).actionGet(TIMEOUT);
        } catch (ResourceNotFoundException ignored) {
            // none created by this test
        } catch (Exception e) {
            logger.warn("view cleanup failed", e);
        }
    }

    public void testSubqueryOnlyDataset() {
        registerEmployees();
        try (var response = run(syncEsqlQueryRequest("FROM (FROM employees) | SORT emp_no"), TIMEOUT)) {
            validateOutput(response, 3);
        }
    }

    public void testMainDatasetWithSubqueryDataset() {
        registerEmployees();
        registerEmployeesAlt();
        try (var response = run(syncEsqlQueryRequest("FROM employees, (FROM employees_alt) | SORT emp_no"), TIMEOUT)) {
            validateOutput(response, 5);
        }
    }

    public void testMultipleSubqueriesOnDatasets() {
        registerEmployees();
        registerEmployeesAlt();
        try (var response = run(syncEsqlQueryRequest("FROM (FROM employees), (FROM employees_alt) | SORT emp_no"), TIMEOUT)) {
            validateOutput(response, 5);
        }
    }

    public void testIndexInMainDatasetInSubquery() {
        createRealEmployees();
        registerEmployees();
        registerEmployeesAlt();

        // real_employees emp_no {1, 3, 99, 100, 101} overlaps employees {1, 2, 3} on {1, 3}, so the union carries two rows at emp_no 1
        // and 3. A secondary SORT on first_name makes their order deterministic: the dataset's "Alice"/"Carol" sort ahead of the index's
        // suffixed "Alice-real"/"Carol-real".
        try (
            var response = run(
                syncEsqlQueryRequest("FROM real_employees, (FROM employees), (FROM employees_alt) | SORT emp_no, first_name"),
                TIMEOUT
            )
        ) {
            List<? extends ColumnInfo> columns = response.columns();
            assertThat(columns, hasSize(5));
            assertThat(columns.get(0).name(), equalTo("emp_no"));
            assertThat(columns.get(1).name(), equalTo("first_name"));
            assertThat(columns.get(2).name(), equalTo("last_name"));
            assertThat(columns.get(3).name(), equalTo("department"));
            assertThat(columns.get(4).name(), equalTo("salary"));

            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(10));

            assertThat(rows.get(0).get(0), equalTo(1));
            assertThat(rows.get(0).get(1).toString(), equalTo("Alice"));
            assertThat(rows.get(0).get(2).toString(), equalTo("Anderson"));
            assertThat(rows.get(0).get(3).toString(), equalTo("Engineering"));
            assertThat(rows.get(0).get(4), equalTo(50000));

            assertThat(rows.get(1).get(0), equalTo(1));
            assertThat(rows.get(1).get(1).toString(), equalTo("Alice-real"));
            assertNull(rows.get(1).get(2));
            assertNull(rows.get(1).get(3));
            assertNull(rows.get(1).get(4));

            assertThat(rows.get(2).get(0), equalTo(2));
            assertThat(rows.get(2).get(1).toString(), equalTo("Bob"));

            assertThat(rows.get(3).get(0), equalTo(3));
            assertThat(rows.get(3).get(1).toString(), equalTo("Carol"));
            assertThat(rows.get(3).get(3).toString(), equalTo("Sales"));

            assertThat(rows.get(4).get(0), equalTo(3));
            assertThat(rows.get(4).get(1).toString(), equalTo("Carol-real"));
            assertNull(rows.get(4).get(3));

            assertThat(rows.get(5).get(0), equalTo(10));
            assertThat(rows.get(5).get(1).toString(), equalTo("Diana"));

            assertThat(rows.get(6).get(0), equalTo(11));
            assertThat(rows.get(6).get(1).toString(), equalTo("Eve"));

            assertThat(rows.get(7).get(0), equalTo(99));
            assertThat(rows.get(7).get(1).toString(), equalTo("Zach-real"));
            assertNull(rows.get(7).get(2));

            assertThat(rows.get(8).get(0), equalTo(100));
            assertThat(rows.get(8).get(1).toString(), equalTo("Frank"));
            assertNull(rows.get(8).get(3));

            assertThat(rows.get(9).get(0), equalTo(101));
            assertThat(rows.get(9).get(1).toString(), equalTo("Grace"));
            assertNull(rows.get(9).get(4));
        }
    }

    private void validateOutput(EsqlQueryResponse response, int resultSize) {
        List<? extends ColumnInfo> columns = response.columns();
        // Five columns from the fixture header — emp_no, first_name, last_name, department, salary.
        assertThat(columns, hasSize(5));
        assertThat(columns.get(0).name(), equalTo("emp_no"));
        assertThat(columns.get(1).name(), equalTo("first_name"));
        assertThat(columns.get(2).name(), equalTo("last_name"));
        assertThat(columns.get(3).name(), equalTo("department"));
        assertThat(columns.get(4).name(), equalTo("salary"));

        List<List<Object>> rows = getValuesList(response);
        assertThat(rows, hasSize(resultSize));
        assertThat(rows.get(0).get(0), equalTo(1));
        assertThat(rows.get(0).get(1).toString(), equalTo("Alice"));
        assertThat(rows.get(0).get(2).toString(), equalTo("Anderson"));
        assertThat(rows.get(0).get(3).toString(), equalTo("Engineering"));
        assertThat(rows.get(0).get(4), equalTo(50000));

        assertThat(rows.get(1).get(0), equalTo(2));
        assertThat(rows.get(1).get(1).toString(), equalTo("Bob"));
        assertThat(rows.get(1).get(2).toString(), equalTo("Brown"));
        assertThat(rows.get(1).get(3).toString(), equalTo("Engineering"));
        assertThat(rows.get(1).get(4), equalTo(60000));

        assertThat(rows.get(2).get(0), equalTo(3));
        assertThat(rows.get(2).get(1).toString(), equalTo("Carol"));
        assertThat(rows.get(2).get(2).toString(), equalTo("Cox"));
        assertThat(rows.get(2).get(3).toString(), equalTo("Sales"));
        assertThat(rows.get(2).get(4), equalTo(55000));

        if (resultSize > 3) {
            assertThat(rows.get(3).get(0), equalTo(10));
            assertThat(rows.get(3).get(1).toString(), equalTo("Diana"));
            assertThat(rows.get(3).get(2).toString(), equalTo("Davis"));
            assertThat(rows.get(3).get(3).toString(), equalTo("Engineering"));
            assertThat(rows.get(3).get(4), equalTo(75000));

            assertThat(rows.get(4).get(0), equalTo(11));
            assertThat(rows.get(4).get(1).toString(), equalTo("Eve"));
            assertThat(rows.get(4).get(2).toString(), equalTo("Evans"));
            assertThat(rows.get(4).get(3).toString(), equalTo("Sales"));
            assertThat(rows.get(4).get(4), equalTo(65000));
        }
    }

    private void validateSalaryUnion(EsqlQueryResponse response) {
        List<? extends ColumnInfo> columns = response.columns();
        assertThat(columns, hasSize(3));
        assertThat(columns.get(0).name(), equalTo("emp_no"));
        assertThat(columns.get(1).name(), equalTo("name"));
        assertThat(columns.get(2).name(), equalTo("salary"));

        List<List<Object>> rows = getValuesList(response);
        assertThat(rows, hasSize(4));

        assertThat(rows.get(0).get(0), equalTo(1));
        assertThat(rows.get(0).get(1).toString(), equalTo("Alice"));
        assertThat(((Number) rows.get(0).get(2)).longValue(), equalTo(50000L));

        assertThat(rows.get(1).get(0), equalTo(2));
        assertThat(rows.get(1).get(1).toString(), equalTo("Bob"));
        assertThat(((Number) rows.get(1).get(2)).longValue(), equalTo(60000L));

        assertThat(rows.get(2).get(0), equalTo(11));
        assertThat(rows.get(2).get(1).toString(), equalTo("Eve"));
        assertThat(((Number) rows.get(2).get(2)).longValue(), equalTo(65000L));

        assertThat(rows.get(3).get(0), equalTo(10));
        assertThat(rows.get(3).get(1).toString(), equalTo("Diana"));
        assertThat(((Number) rows.get(3).get(2)).longValue(), equalTo(75000L));
    }

    /**
     * A subquery that mixes a real index with a dataset is allowed — {@code DatasetRewriter} builds a
     * {@code UnionAll} for the inner {@code FROM real_employees, employees} instead of rejecting it.
     * The outer query sees a single subquery result; real_employees rows carry nulls for the dataset-only columns.
     */
    public void testMixedTargetsInSubqueryAllowed() {
        createRealEmployees();
        registerEmployees();

        try (var response = run(syncEsqlQueryRequest("FROM (FROM real_employees, employees) | SORT emp_no, first_name"), TIMEOUT)) {
            List<? extends ColumnInfo> columns = response.columns();
            assertThat(columns, hasSize(5));

            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(8)); // 5 from real_employees + 3 from employees

            int empNoIdx = columns.stream().map(ColumnInfo::name).toList().indexOf("emp_no");
            int firstNameIdx = columns.stream().map(ColumnInfo::name).toList().indexOf("first_name");
            int lastNameIdx = columns.stream().map(ColumnInfo::name).toList().indexOf("last_name");

            // emp_no=1: dataset row (Alice) sorts before index row (Alice-real)
            assertThat(rows.get(0).get(empNoIdx), equalTo(1));
            assertThat(rows.get(0).get(firstNameIdx).toString(), equalTo("Alice"));
            assertThat(rows.get(0).get(lastNameIdx).toString(), equalTo("Anderson"));
            assertThat(rows.get(1).get(empNoIdx), equalTo(1));
            assertThat(rows.get(1).get(firstNameIdx).toString(), equalTo("Alice-real"));
            assertNull(rows.get(1).get(lastNameIdx)); // real_employees has no last_name

            // emp_no=2: dataset only
            assertThat(rows.get(2).get(empNoIdx), equalTo(2));
            assertThat(rows.get(2).get(firstNameIdx).toString(), equalTo("Bob"));

            // emp_no=3: dataset row (Carol) then index row (Carol-real)
            assertThat(rows.get(3).get(empNoIdx), equalTo(3));
            assertThat(rows.get(3).get(firstNameIdx).toString(), equalTo("Carol"));
            assertThat(rows.get(4).get(empNoIdx), equalTo(3));
            assertThat(rows.get(4).get(firstNameIdx).toString(), equalTo("Carol-real"));
            assertNull(rows.get(4).get(lastNameIdx));

            // emp_no >= 99: real_employees only, null-padded
            assertThat(rows.get(5).get(empNoIdx), equalTo(99));
            assertNull(rows.get(5).get(lastNameIdx));
            assertThat(rows.get(6).get(empNoIdx), equalTo(100));
            assertThat(rows.get(7).get(empNoIdx), equalTo(101));
        }
    }

    public void testIndexInMainMultipleDatasetInSubqueryRejected() {
        createRealEmployees();
        registerEmployees();
        registerEmployeesAlt();

        Exception ex = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM real_employees, (FROM employees, employees_alt)"), TIMEOUT)
        );
        assertCauseMessageContains(ex, "Nested subqueries are not supported");
    }

    // With basic(WHERE/STATS/KEEP/EVAL) processing command in subqueries or main query

    public void testMainDatasetWithSubqueryDatasetWithStatsInMainQuery() {
        registerEmployees();
        registerEmployeesAlt();

        try (var response = run(syncEsqlQueryRequest("FROM employees, (FROM employees_alt) | STATS c = COUNT(*)"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            // 3 from employees + 2 from employees_alt
            assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(5L));
        }
    }

    public void testMultipleSubqueriesOnDatasetsWithStatsInMainQuery() {
        registerEmployees();
        registerEmployeesAlt();

        try (var response = run(syncEsqlQueryRequest("FROM (FROM employees), (FROM employees_alt) | STATS c = COUNT(*)"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(5L));
        }
    }

    public void testIndexInMainDatasetInSubqueryWithStatsInMainQuery() {
        createRealEmployees();
        registerEmployees();

        try (var response = run(syncEsqlQueryRequest("""
            FROM real_employees, (FROM employees)
            | STATS c = COUNT(*)
            """), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            // 5 from real_employees + 3 from employees
            assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(8L));
        }
    }

    public void testFilterInSubquery() {
        registerEmployees();
        registerEmployeesAlt();

        try (var response = run(syncEsqlQueryRequest("""
            FROM (FROM employees | WHERE emp_no > 1), (FROM employees_alt | WHERE emp_no < 11)
            | SORT emp_no
            """), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            assertThat(rows.get(0).get(0), equalTo(2));
            assertThat(rows.get(0).get(1).toString(), equalTo("Bob"));
            assertThat(rows.get(1).get(0), equalTo(3));
            assertThat(rows.get(1).get(1).toString(), equalTo("Carol"));
            assertThat(rows.get(2).get(0), equalTo(10));
            assertThat(rows.get(2).get(1).toString(), equalTo("Diana"));
        }
    }

    public void testFilterInMainQuery() {
        createRealEmployees();
        registerEmployees();
        registerEmployeesAlt();

        try (var response = run(syncEsqlQueryRequest("""
            FROM real_employees, (FROM employees), (FROM employees_alt)
            | WHERE department == "Sales"
            | SORT emp_no
            """), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(2));
            assertThat(rows.get(0).get(0), equalTo(3));
            assertThat(rows.get(0).get(1).toString(), equalTo("Carol"));
            assertThat(rows.get(1).get(0), equalTo(11));
            assertThat(rows.get(1).get(1).toString(), equalTo("Eve"));
        }
    }

    public void testKeepInSubquery() {
        registerEmployees();
        registerEmployeesAlt();

        try (var response = run(syncEsqlQueryRequest("""
            FROM (FROM employees | KEEP first_name), (FROM employees_alt | KEEP first_name) | SORT first_name
            """), TIMEOUT)) {
            List<? extends ColumnInfo> columns = response.columns();
            assertThat(columns, hasSize(1));
            assertThat(columns.get(0).name(), equalTo("first_name"));

            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(5));
            assertThat(rows.get(0).get(0).toString(), equalTo("Alice"));
            assertThat(rows.get(1).get(0).toString(), equalTo("Bob"));
            assertThat(rows.get(2).get(0).toString(), equalTo("Carol"));
            assertThat(rows.get(3).get(0).toString(), equalTo("Diana"));
            assertThat(rows.get(4).get(0).toString(), equalTo("Eve"));
        }
    }

    public void testKeepInMainQuery() {
        createRealEmployees();
        registerEmployees();
        registerEmployeesAlt();

        try (var response = run(syncEsqlQueryRequest("""
            FROM real_employees, (FROM employees), (FROM employees_alt)
            | EVAL first_name = first_name::string
            | KEEP first_name
            | SORT first_name
            """), TIMEOUT)) {
            List<? extends ColumnInfo> columns = response.columns();
            assertThat(columns, hasSize(1));
            assertThat(columns.get(0).name(), equalTo("first_name"));

            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(10));
            assertThat(rows.get(0).get(0).toString(), equalTo("Alice"));
            assertThat(rows.get(1).get(0).toString(), equalTo("Alice-real"));
            assertThat(rows.get(2).get(0).toString(), equalTo("Bob"));
            assertThat(rows.get(3).get(0).toString(), equalTo("Carol"));
            assertThat(rows.get(4).get(0).toString(), equalTo("Carol-real"));
            assertThat(rows.get(5).get(0).toString(), equalTo("Diana"));
            assertThat(rows.get(6).get(0).toString(), equalTo("Eve"));
            assertThat(rows.get(7).get(0).toString(), equalTo("Frank"));
            assertThat(rows.get(8).get(0).toString(), equalTo("Grace"));
            assertThat(rows.get(9).get(0).toString(), equalTo("Zach-real"));
        }
    }

    public void testStatsInSubquery() {
        registerEmployees();
        registerEmployeesAlt();

        try (var response = run(syncEsqlQueryRequest("""
            FROM (FROM employees | STATS c = COUNT(*)), (FROM employees_alt | STATS c = COUNT(*))
            | SORT c
            """), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(2));
            assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(2L));
            assertThat(((Number) rows.get(1).get(0)).longValue(), equalTo(3L));
        }
    }

    public void testEvalInSubquery() {
        registerEmployees();
        registerEmployeesAlt();

        try (var response = run(syncEsqlQueryRequest("""
            FROM (FROM employees | EVAL doubled = emp_no * 2), (FROM employees_alt | EVAL doubled = emp_no * 2)
            | SORT emp_no
            | LIMIT 1
            """), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            int empNoIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("emp_no");
            int doubledIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("doubled");
            assertThat(rows.get(0).get(empNoIdx), equalTo(1));
            assertThat(((Number) rows.get(0).get(doubledIdx)).intValue(), equalTo(2));
        }
    }

    public void testEvalInMainQuery() {
        registerEmployees();
        registerEmployeesAlt();

        try (var response = run(syncEsqlQueryRequest("""
            FROM (FROM employees), (FROM employees_alt)
            | EVAL doubled = emp_no * 2
            | SORT emp_no
            | LIMIT 1
            """), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            int empNoIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("emp_no");
            int doubledIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("doubled");
            assertThat(rows.get(0).get(empNoIdx), equalTo(1));
            assertThat(((Number) rows.get(0).get(doubledIdx)).intValue(), equalTo(2));
        }
    }

    public void testMetadataOnDatasetInSubquery() {
        registerEmployees();

        // Standard metadata binds on a dataset inside a subquery, consistent with how it binds on a
        // regular index in the same position (see IndexResolutionIT / subquery.csv-spec). _index
        // resolves to the dataset name. This used to be rejected only because dataset metadata was
        // unsupported anywhere; it is supported now. KEEP is irrelevant to metadata presence on the
        // FROM path: METADATA _index surfaces _index with no explicit KEEP.
        try (var response = run(syncEsqlQueryRequest("FROM (FROM employees METADATA _index) | LIMIT 1"), TIMEOUT)) {
            List<String> names = response.columns().stream().map(ColumnInfo::name).toList();
            assertThat("_index must surface without an explicit KEEP, got " + names, names, hasItem("_index"));

            int idx = names.indexOf("_index");
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(rows.get(0).get(idx).toString(), equalTo("employees"));
        }
    }

    public void testFileMetadataOnDatasetInSubquery() {
        registerEmployees();

        // KEEP is irrelevant to metadata presence on the FROM path: METADATA _file.path must surface
        // _file.path in the output with NO explicit KEEP, inside a subquery too.
        try (var response = run(syncEsqlQueryRequest("FROM (FROM employees METADATA _file.path) | LIMIT 1"), TIMEOUT)) {
            List<? extends ColumnInfo> columns = response.columns();
            List<String> names = columns.stream().map(ColumnInfo::name).toList();
            assertThat("_file.path must surface without an explicit KEEP, got " + names, names, hasItem("_file.path"));

            int idx = names.indexOf("_file.path");
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.get(0).get(idx), notNullValue());
            assertThat(rows.get(0).get(idx).toString(), containsString(".csv"));
        }
    }

    public void testMetadataSurvivesDeepSubqueryNesting() {
        registerEmployees();

        // Surfacing is independent of subquery depth: METADATA named two levels down still reaches the
        // top-level output with no KEEP at any level. Mixes standard (_index) and file (_file.path)
        // metadata in one clause to prove both families ride the same no-KEEP path.
        try (var response = run(syncEsqlQueryRequest("FROM (FROM (FROM employees METADATA _index, _file.path)) | LIMIT 1"), TIMEOUT)) {
            List<String> names = response.columns().stream().map(ColumnInfo::name).toList();
            assertThat("_index must surface through nested subqueries without KEEP, got " + names, names, hasItem("_index"));
            assertThat("_file.path must surface through nested subqueries without KEEP, got " + names, names, hasItem("_file.path"));

            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(rows.get(0).get(names.indexOf("_index")).toString(), equalTo("employees"));
            assertThat(rows.get(0).get(names.indexOf("_file.path")).toString(), containsString(".csv"));
        }
    }

    public void testMetadataSurfacesThroughViewOverDataset() {
        registerEmployees();
        createView("emp_meta_view", "FROM employees METADATA _index, _file.path");

        // A view is a saved query; FROM <view> expands to it, so metadata named in the view's body
        // surfaces at the call site with no KEEP — directly and inside a subquery, matching the FROM
        // contract. The view body carries the METADATA clause; the caller adds nothing.
        for (String query : List.of("FROM emp_meta_view | LIMIT 1", "FROM (FROM emp_meta_view) | LIMIT 1")) {
            try (var response = run(syncEsqlQueryRequest(query), TIMEOUT)) {
                List<String> names = response.columns().stream().map(ColumnInfo::name).toList();
                assertThat(query + " must surface _index without KEEP, got " + names, names, hasItem("_index"));
                assertThat(query + " must surface _file.path without KEEP, got " + names, names, hasItem("_file.path"));

                List<List<Object>> rows = getValuesList(response);
                assertThat(rows.get(0).get(names.indexOf("_index")).toString(), equalTo("employees"));
                assertThat(rows.get(0).get(names.indexOf("_file.path")).toString(), containsString(".csv"));
            }
        }
    }

    // More processing pipelines inside the subquery

    public void testStatsByGroupingInSubquery() {
        registerEmployees();
        registerEmployeesAlt();

        try (var response = run(syncEsqlQueryRequest("""
            FROM
                (FROM employees | STATS total = SUM(salary), c = COUNT(*) BY department),
                (FROM employees_alt | STATS total = SUM(salary), c = COUNT(*) BY department)
            | SORT department, c desc, total
            """), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            int totalIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("total");
            int cIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("c");
            int deptIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("department");
            assertThat(rows, hasSize(4));
            assertThat(rows.get(0).get(deptIdx).toString(), equalTo("Engineering"));
            assertThat(((Number) rows.get(0).get(totalIdx)).longValue(), equalTo(110000L));
            assertThat(((Number) rows.get(0).get(cIdx)).longValue(), equalTo(2L));
            assertThat(rows.get(1).get(deptIdx).toString(), equalTo("Engineering"));
            assertThat(((Number) rows.get(1).get(totalIdx)).longValue(), equalTo(75000L));
            assertThat(((Number) rows.get(1).get(cIdx)).longValue(), equalTo(1L));
            assertThat(rows.get(2).get(deptIdx).toString(), equalTo("Sales"));
            assertThat(((Number) rows.get(2).get(totalIdx)).longValue(), equalTo(55000L));
            assertThat(((Number) rows.get(2).get(cIdx)).longValue(), equalTo(1L));
            assertThat(rows.get(3).get(deptIdx).toString(), equalTo("Sales"));
            assertThat(((Number) rows.get(3).get(totalIdx)).longValue(), equalTo(65000L));
            assertThat(((Number) rows.get(3).get(cIdx)).longValue(), equalTo(1L));
        }
    }

    public void testSortAndLimitInSubquery() {
        registerEmployees();
        registerEmployeesAlt();

        try (var response = run(syncEsqlQueryRequest("""
            FROM
                (FROM employees | SORT salary DESC | LIMIT 2),
                (FROM employees_alt | SORT salary | LIMIT 1)
            | SORT salary DESC
            """), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            int empNoIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("emp_no");
            int salaryIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("salary");
            assertThat(rows, hasSize(3));
            assertThat(rows.get(0).get(empNoIdx), equalTo(11));
            assertThat(((Number) rows.get(0).get(salaryIdx)).intValue(), equalTo(65000));
            assertThat(rows.get(1).get(empNoIdx), equalTo(2));
            assertThat(((Number) rows.get(1).get(salaryIdx)).intValue(), equalTo(60000));
            assertThat(rows.get(2).get(empNoIdx), equalTo(3));
            assertThat(((Number) rows.get(2).get(salaryIdx)).intValue(), equalTo(55000));
        }
    }

    public void testEvalWhereChainInSubquery() {
        registerEmployees();
        registerEmployeesAlt();

        try (var response = run(syncEsqlQueryRequest("""
            FROM
                (FROM employees | EVAL bonus = salary * 0.1 | WHERE bonus > 5500 | KEEP emp_no, bonus),
                (FROM employees_alt | EVAL bonus = salary * 0.1 | WHERE bonus > 5500 | KEEP emp_no, bonus)
            | SORT emp_no
            """), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            int empNoIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("emp_no");
            int bonusIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("bonus");
            assertThat(rows.get(0).get(empNoIdx), equalTo(2));
            assertThat(((Number) rows.get(0).get(bonusIdx)).doubleValue(), equalTo(6000.0));
            assertThat(rows.get(1).get(empNoIdx), equalTo(10));
            assertThat(((Number) rows.get(1).get(bonusIdx)).doubleValue(), equalTo(7500.0));
            assertThat(rows.get(2).get(empNoIdx), equalTo(11));
            assertThat(((Number) rows.get(2).get(bonusIdx)).doubleValue(), equalTo(6500.0));
        }
    }

    public void testDissectInSubquery() {
        registerEmployees();
        registerEmployeesAlt();

        try (var response = run(syncEsqlQueryRequest("""
            FROM
                (FROM employees | DISSECT first_name "%{first_letter}lice" | KEEP emp_no, first_letter),
                (FROM employees_alt | DISSECT first_name "%{first_letter}lice" | KEEP emp_no, first_letter)
            | SORT emp_no
            """), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(5));
            int letterIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("first_letter");
            long matches = rows.stream().filter(r -> r.get(letterIdx) != null && r.get(letterIdx).toString().equals("A")).count();
            assertThat(matches, equalTo(1L));
        }
    }

    public void testLookupJoinInSubquery() {
        registerEmployees();
        registerEmployeesAlt();
        createDepartmentsLookup();

        try (var response = run(syncEsqlQueryRequest("""
            FROM
                (FROM employees | LOOKUP JOIN departments_lookup ON department | KEEP emp_no, department, location),
                (FROM employees_alt | LOOKUP JOIN departments_lookup ON department | KEEP emp_no, department, location)
            | SORT emp_no
            """), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(5));
            int locIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("location");
            assertThat(rows.get(0).get(locIdx).toString(), equalTo("Mountain View")); // Alice / Engineering
            assertThat(rows.get(1).get(locIdx).toString(), equalTo("Mountain View")); // Bob / Engineering
            assertThat(rows.get(2).get(locIdx).toString(), equalTo("New York"));      // Carol / Sales
            assertThat(rows.get(3).get(locIdx).toString(), equalTo("Mountain View"));
            assertThat(rows.get(4).get(locIdx).toString(), equalTo("New York"));
        }
    }

    public void testInlineStatsInSubquery() {
        registerEmployees();
        registerEmployeesAlt();

        try (var response = run(syncEsqlQueryRequest("""
            FROM (FROM employees
                  | INLINE STATS max_salary = MAX(salary)
                  | WHERE salary == max_salary
                  | KEEP emp_no, salary, max_salary),
                  (FROM employees_alt
                  | INLINE STATS max_salary = MAX(salary)
                  | WHERE salary == max_salary
                  | KEEP emp_no, salary, max_salary)
            | SORT emp_no
            """), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(2));
            int empNoIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("emp_no");
            int maxIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("max_salary");
            assertThat(rows.get(0).get(empNoIdx), equalTo(2));
            assertThat(((Number) rows.get(0).get(maxIdx)).intValue(), equalTo(60000));
            assertThat(rows.get(1).get(empNoIdx), equalTo(10));
            assertThat(((Number) rows.get(1).get(maxIdx)).intValue(), equalTo(75000));
        }
    }

    // More processing commands in main query

    public void testStatsByAfterSubquery() {
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

    public void testWhereAndStatsAfterSubquery() {
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
     * {@code testSubqueryWithLookupJoin} from the LOOKUP JOIN operator's point of view; what's
     * pinned here is that JOIN composes with the outer UnionAll output without reaching back into
     * the dataset relation.
     */
    public void testLookupJoinAfterSubquery() {
        registerEmployees();
        registerEmployeesAlt();
        createDepartmentsLookup();

        try (var response = run(syncEsqlQueryRequest("""
             FROM (FROM employees), (FROM employees_alt)
             | LOOKUP JOIN departments_lookup ON department
             | KEEP emp_no, department, location
             | SORT emp_no
            """), TIMEOUT)) {
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

    public void testInlineStatsAfterSubquery() {
        registerEmployees();
        registerEmployeesAlt();

        try (var response = run(syncEsqlQueryRequest("""
            FROM (FROM employees), (FROM employees_alt)
            | INLINE STATS max_salary = MAX(salary)
            | WHERE salary == max_salary
            | KEEP emp_no, salary, max_salary
            """), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            // Across both datasets, Diana (75000) holds the max.
            assertThat(rows, hasSize(1));
            int empNoIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("emp_no");
            int maxIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("max_salary");
            assertThat(rows.get(0).get(empNoIdx), equalTo(10));
            assertThat(((Number) rows.get(0).get(maxIdx)).intValue(), equalTo(75000));
        }
    }

    public void testEvalSortLimitAfterSubquery() {
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

    public void testMatchAfterSubquery() {
        registerEmployees();
        registerEmployeesAlt();

        try (var response = run(syncEsqlQueryRequest("""
            FROM (FROM employees METADATA _index), (FROM employees_alt METADATA _index)
            | WHERE MATCH(first_name, "Alice")
            | KEEP first_name, last_name, _index
            | SORT _index
            """), TIMEOUT)) {
            assertColumnNames(response.columns(), List.of("first_name", "last_name", "_index"));
            assertColumnTypes(response.columns(), List.of("keyword", "keyword", "keyword"));
            assertValues(response.values(), List.of(List.of("Alice", "Anderson", "employees")));
        }
    }

    // Full-text functions against external data sources
    public void testMatchOnDatasetField() {
        registerEmployees();
        try (var response = run(syncEsqlQueryRequest("""
            FROM (FROM employees | WHERE MATCH(first_name, "Alice"))
            | KEEP first_name, last_name
            """), TIMEOUT)) {
            assertColumnNames(response.columns(), List.of("first_name", "last_name"));
            assertColumnTypes(response.columns(), List.of("keyword", "keyword"));
            assertValues(response.values(), List.of(List.of("Alice", "Anderson")));
        }
    }

    /**
     * MATCH_PHRASE on a dataset (keyword) field works via runtime search, matching the exact value like the term
     * query a pushed-down match_phrase on a keyword field rewrites to.
     */
    public void testMatchPhraseOnDatasetField() {
        registerEmployees();

        try (var response = run(syncEsqlQueryRequest("""
            FROM (FROM employees | WHERE MATCH_PHRASE(first_name, "Alice"))
            | KEEP first_name, last_name
            """), TIMEOUT)) {
            assertColumnNames(response.columns(), List.of("first_name", "last_name"));
            assertValues(response.values(), List.of(List.of("Alice", "Anderson")));
        }
    }

    public void testKQLOnDatasetRejected() {
        registerEmployees();

        Exception ex = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM (FROM employees | WHERE KQL(\"first_name: Alice\"))"), TIMEOUT)
        );
        assertCauseMessageContains(
            ex,
            "[KQL] function is not supported on federated data sources [employees]; it requires an index. "
                + "Use MATCH(field, \"term\") for full-text search on non-indexed data."
        );
    }

    public void testQSTROnDatasetRejected() {
        registerEmployees();

        Exception ex = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM (FROM employees | WHERE QSTR(\"first_name: Alice\"))"), TIMEOUT)
        );
        assertCauseMessageContains(
            ex,
            "[QSTR] function is not supported on federated data sources [employees]; it requires an index. "
                + "Use MATCH(field, \"term\") for full-text search on non-indexed data."
        );
    }

    /**
     * MATCH_PHRASE after a subquery union of datasets works via runtime search, mirroring
     * {@link #testMatchAfterSubquery}.
     */
    public void testMatchPhraseAfterSubquery() {
        registerEmployees();
        registerEmployeesAlt();

        try (var response = run(syncEsqlQueryRequest("""
            FROM (FROM employees), (FROM employees_alt) | WHERE MATCH_PHRASE(first_name, "Alice")
            | KEEP first_name, last_name
            """), TIMEOUT)) {
            assertColumnNames(response.columns(), List.of("first_name", "last_name"));
            assertValues(response.values(), List.of(List.of("Alice", "Anderson")));
        }
    }

    // Mixed data types across subquery branches

    public void testConflictingTypesAcrossSubqueriesRejected() {
        registerSalariesInt();
        registerSalariesLong();

        Exception ex = expectThrows(Exception.class, () -> run(syncEsqlQueryRequest("""
            FROM (FROM salaries_int), (FROM salaries_long)
            | SORT salary
            """), TIMEOUT));
        assertCauseMessageContains(ex, "Column [salary] has conflicting data types in subqueries");
    }

    public void testConflictingTypesResolvedByCastInSubqueries() {
        registerSalariesInt();
        registerSalariesLong();

        try (var response = run(syncEsqlQueryRequest("""
            FROM (FROM salaries_int | EVAL salary = salary::long), (FROM salaries_long)
            | SORT salary
            """), TIMEOUT)) {
            validateSalaryUnion(response);
        }
    }

    public void testConflictingTypesResolvedByCastInMainQuery() {
        registerSalariesInt();
        registerSalariesLong();

        try (var response = run(syncEsqlQueryRequest("""
            FROM (FROM salaries_int), (FROM salaries_long)
            | EVAL salary = salary::long
            | SORT salary
            """), TIMEOUT)) {
            validateSalaryUnion(response);
        }
    }

    public void testSubqueryUnionOfIndexTimeSeriesRateAndDataset() {
        assumeTrue("Requires TS source inside a FROM subquery", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        createRealEmployees();
        createTimeSeriesCounters();
        registerEmployees();

        // One row per branch, aliased to a common keyword `name`: real_employees emp_no 100 -> "Frank",
        // ts_counters department "Sales", employees dataset emp_no 2 -> "Bob". Sorted ascending: Bob, Frank, Sales.
        try (var response = run(syncEsqlQueryRequest("""
            FROM (FROM real_employees | WHERE emp_no == 100 | EVAL name = first_name | KEEP name),
                 (TS ts_counters
                  | STATS max_rate = max(rate(requests)) BY department
                  | WHERE department == "Sales"
                  | EVAL name = department
                  | KEEP name),
                 (FROM employees | WHERE emp_no == 2 | EVAL name = first_name | KEEP name)
            | SORT name
            """), TIMEOUT)) {
            List<? extends ColumnInfo> columns = response.columns();
            assertThat(columns, hasSize(1));
            assertThat(columns.get(0).name(), equalTo("name"));

            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            assertThat(rows.get(0).get(0).toString(), equalTo("Bob"));
            assertThat(rows.get(1).get(0).toString(), equalTo("Frank"));
            assertThat(rows.get(2).get(0).toString(), equalTo("Sales"));
        }
    }

    // WHERE ... IN / NOT IN (subquery) crossed with external datasets

    public void testInSubqueryMainDatasetSubqueryIndex() {
        requireInSubquery();
        registerEmployees();
        createRealEmployees();

        try (var response = run(syncEsqlQueryRequest("""
            FROM employees
            | WHERE emp_no IN (FROM real_employees | KEEP emp_no)
            | SORT emp_no
            | KEEP emp_no
            """), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(2));
            assertThat(rows.get(0).get(0), equalTo(1));
            assertThat(rows.get(1).get(0), equalTo(3));
        }
    }

    public void testNotInSubqueryMainDatasetSubqueryIndex() {
        requireInSubquery();
        registerEmployees();
        createRealEmployees();

        try (var response = run(syncEsqlQueryRequest("""
            FROM employees
            | WHERE emp_no NOT IN (FROM real_employees | KEEP emp_no)
            | SORT emp_no
            | KEEP emp_no
            """), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(rows.get(0).get(0), equalTo(2));
        }
    }

    public void testInSubqueryMainIndexSubqueryDataset() {
        requireInSubquery();
        registerEmployees();
        createRealEmployees();

        try (var response = run(syncEsqlQueryRequest("""
            FROM real_employees
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
            | SORT emp_no
            | KEEP emp_no
            """), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(2));
            assertThat(rows.get(0).get(0), equalTo(1));
            assertThat(rows.get(1).get(0), equalTo(3));
        }
    }

    public void testNotInSubqueryMainIndexSubqueryDataset() {
        requireInSubquery();
        registerEmployees();
        createRealEmployees();

        try (var response = run(syncEsqlQueryRequest("""
            FROM real_employees
            | WHERE emp_no NOT IN (FROM employees | KEEP emp_no)
            | SORT emp_no
            | KEEP emp_no
            """), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            assertThat(rows.get(0).get(0), equalTo(99));
            assertThat(rows.get(1).get(0), equalTo(100));
            assertThat(rows.get(2).get(0), equalTo(101));
        }
    }

    public void testInSubqueryMainDatasetSubqueryDataset() {
        requireInSubquery();
        registerEmployees();
        registerEmployeesAlt();

        try (var response = run(syncEsqlQueryRequest("""
            FROM employees
            | WHERE department IN (FROM employees_alt | WHERE first_name == "Eve" | KEEP department)
            | SORT emp_no
            | KEEP emp_no, department
            """), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(rows.get(0).get(0), equalTo(3));
            assertThat(rows.get(0).get(1).toString(), equalTo("Sales"));
        }
    }

    public void testNotInSubqueryMainDatasetSubqueryDataset() {
        requireInSubquery();
        registerEmployees();
        registerEmployeesAlt();

        try (var response = run(syncEsqlQueryRequest("""
            FROM employees
            | WHERE department NOT IN (FROM employees_alt | WHERE first_name == "Eve" | KEEP department)
            | SORT emp_no
            | KEEP emp_no, department
            """), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(2));
            assertThat(rows.get(0).get(0), equalTo(1));
            assertThat(rows.get(0).get(1).toString(), equalTo("Engineering"));
            assertThat(rows.get(1).get(0), equalTo(2));
            assertThat(rows.get(1).get(1).toString(), equalTo("Engineering"));
        }
    }

    // WHERE ... IN / NOT IN (subquery) crossing a time-series index with an external dataset

    public void testInSubqueryMainTimeSeriesIndexSubqueryDataset() {
        requireInSubquery();
        registerEmployees();
        createTimeSeriesMetrics();

        try (var response = run(syncEsqlQueryRequest("""
            FROM ts_metrics
            | WHERE department IN (FROM employees | KEEP department)
            | KEEP department
            | SORT department
            """), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(rows.get(0).get(0).toString(), equalTo("Sales"));
        }
    }

    public void testNotInSubqueryMainTimeSeriesIndexSubqueryDataset() {
        requireInSubquery();
        registerEmployees();
        createTimeSeriesMetrics();

        try (var response = run(syncEsqlQueryRequest("""
            FROM ts_metrics
            | WHERE department NOT IN (FROM employees | KEEP department)
            | KEEP department
            | SORT department
            """), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(rows.get(0).get(0).toString(), equalTo("Marketing"));
        }
    }

    public void testInSubqueryMainDatasetSubqueryTimeSeriesIndex() {
        requireInSubquery();
        registerEmployees();
        createTimeSeriesMetrics();

        try (var response = run(syncEsqlQueryRequest("""
            FROM employees
            | WHERE department IN (FROM ts_metrics | KEEP department)
            | SORT emp_no
            | KEEP emp_no, department
            """), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(rows.get(0).get(0), equalTo(3));
            assertThat(rows.get(0).get(1).toString(), equalTo("Sales"));
        }
    }

    public void testNotInSubqueryMainDatasetSubqueryTimeSeriesIndex() {
        requireInSubquery();
        registerEmployees();
        createTimeSeriesMetrics();

        try (var response = run(syncEsqlQueryRequest("""
            FROM employees
            | WHERE department NOT IN (FROM ts_metrics | KEEP department)
            | SORT emp_no
            | KEEP emp_no, department
            """), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(2));
            assertThat(rows.get(0).get(0), equalTo(1));
            assertThat(rows.get(0).get(1).toString(), equalTo("Engineering"));
            assertThat(rows.get(1).get(0), equalTo(2));
            assertThat(rows.get(1).get(1).toString(), equalTo("Engineering"));
        }
    }

    // WHERE ... IN / NOT IN (subquery) where the main query reads a time-series index via TS with a rate(...)
    // aggregate (instead of plain FROM) and the subquery references a dataset. Mirrors
    // testInSubqueryMainTimeSeriesIndexSubqueryDataset, exercising the IN/NOT IN join below a lowered TS aggregation.

    public void testInSubqueryMainTimeSeriesRateSubqueryDataset() {
        requireInSubquery();
        requireWhereInSubqueryWithTS();
        registerEmployees();
        createTimeSeriesCounters();

        // employees departments are {Engineering, Sales}; ts_counters carries {Sales, Marketing}, so the IN
        // subquery keeps only Sales before the per-department rate aggregation runs.
        try (var response = run(syncEsqlQueryRequest("""
            TS ts_counters
            | WHERE department IN (FROM employees | KEEP department)
            | STATS max_rate = max(rate(requests)) BY department
            | KEEP department
            | SORT department
            """), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(rows.get(0).get(0).toString(), equalTo("Sales"));
        }
    }

    public void testNotInSubqueryMainTimeSeriesRateSubqueryDataset() {
        requireInSubquery();
        requireWhereInSubqueryWithTS();
        registerEmployees();
        createTimeSeriesCounters();

        // NOT IN keeps the ts_counters departments absent from employees: only Marketing survives.
        try (var response = run(syncEsqlQueryRequest("""
            TS ts_counters
            | WHERE department NOT IN (FROM employees | KEEP department)
            | STATS max_rate = max(rate(requests)) BY department
            | KEEP department
            | SORT department
            """), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(rows.get(0).get(0).toString(), equalTo("Marketing"));
        }
    }

    // WHERE ... IN / NOT IN (subquery) where the main query references a dataset and the subquery reads a time-series
    // index via TS with a rate(...) aggregate (instead of plain FROM). Mirrors testInSubqueryMainDatasetSubqueryTimeSeriesIndex.

    public void testInSubqueryMainDatasetSubqueryTimeSeriesRate() {
        requireInSubquery();
        requireWhereInSubqueryWithTS();
        registerEmployees();
        createTimeSeriesCounters();

        // The TS subquery yields the distinct ts_counters departments {Sales, Marketing}; only Sales also appears in
        // employees, so the IN keeps emp_no 3.
        try (var response = run(syncEsqlQueryRequest("""
            FROM employees
            | WHERE department IN (TS ts_counters | STATS max_rate = max(rate(requests)) BY department | KEEP department)
            | SORT emp_no
            | KEEP emp_no, department
            """), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(rows.get(0).get(0), equalTo(3));
            assertThat(rows.get(0).get(1).toString(), equalTo("Sales"));
        }
    }

    public void testNotInSubqueryMainDatasetSubqueryTimeSeriesRate() {
        requireInSubquery();
        requireWhereInSubqueryWithTS();
        registerEmployees();
        createTimeSeriesCounters();

        // NOT IN drops the employees rows whose department is in {Sales, Marketing}; the two Engineering rows remain.
        try (var response = run(syncEsqlQueryRequest("""
            FROM employees
            | WHERE department NOT IN (TS ts_counters | STATS max_rate = max(rate(requests)) BY department | KEEP department)
            | SORT emp_no
            | KEEP emp_no, department
            """), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(2));
            assertThat(rows.get(0).get(0), equalTo(1));
            assertThat(rows.get(0).get(1).toString(), equalTo("Engineering"));
            assertThat(rows.get(1).get(0), equalTo(2));
            assertThat(rows.get(1).get(1).toString(), equalTo("Engineering"));
        }
    }

    // Helpers

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

    private void createRealEmployees() {
        assertAcked(
            client().admin().indices().prepareCreate("real_employees").setMapping("emp_no", "type=integer", "first_name", "type=keyword")
        );
        ensureGreen("real_employees");
        client().prepareIndex("real_employees").setSource("emp_no", 1, "first_name", "Alice-real").get();
        client().prepareIndex("real_employees").setSource("emp_no", 3, "first_name", "Carol-real").get();
        client().prepareIndex("real_employees").setSource("emp_no", 99, "first_name", "Zach-real").get();
        client().prepareIndex("real_employees").setSource("emp_no", 100, "first_name", "Frank").get();
        client().prepareIndex("real_employees").setSource("emp_no", 101, "first_name", "Grace").get();
        client().admin().indices().prepareRefresh("real_employees").get();
    }

    private void createTimeSeriesMetrics() {
        Settings settings = Settings.builder()
            .put("mode", "time_series")
            .putList("routing_path", List.of("department"))
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .build();
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("ts_metrics")
                .setSettings(settings)
                .setMapping(
                    "@timestamp",
                    "type=date",
                    "department",
                    "type=keyword,time_series_dimension=true",
                    "cpu",
                    "type=double,time_series_metric=gauge"
                )
        );
        ensureGreen("ts_metrics");
        client().prepareIndex("ts_metrics").setSource("@timestamp", "2025-01-01T00:00:00Z", "department", "Sales", "cpu", 0.5).get();
        client().prepareIndex("ts_metrics").setSource("@timestamp", "2025-01-01T00:00:00Z", "department", "Marketing", "cpu", 0.7).get();
        client().admin().indices().prepareRefresh("ts_metrics").get();
    }

    /**
     * A time-series index with a monotonically increasing {@code requests} counter — two samples per department so
     * {@code rate(requests)} has an interval to compute over. Kept separate from {@link #createTimeSeriesMetrics} (which
     * holds a single gauge sample per department) so the existing {@code FROM ts_metrics} tests keep their exact row
     * counts while the rate tests get genuine counter data.
     */
    private void createTimeSeriesCounters() {
        Settings settings = Settings.builder()
            .put("mode", "time_series")
            .putList("routing_path", List.of("department"))
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .build();
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("ts_counters")
                .setSettings(settings)
                .setMapping(
                    "@timestamp",
                    "type=date",
                    "department",
                    "type=keyword,time_series_dimension=true",
                    "requests",
                    "type=long,time_series_metric=counter"
                )
        );
        ensureGreen("ts_counters");
        client().prepareIndex("ts_counters").setSource("@timestamp", "2025-01-01T00:00:00Z", "department", "Sales", "requests", 100L).get();
        client().prepareIndex("ts_counters").setSource("@timestamp", "2025-01-01T00:01:00Z", "department", "Sales", "requests", 160L).get();
        client().prepareIndex("ts_counters")
            .setSource("@timestamp", "2025-01-01T00:00:00Z", "department", "Marketing", "requests", 50L)
            .get();
        client().prepareIndex("ts_counters")
            .setSource("@timestamp", "2025-01-01T00:01:00Z", "department", "Marketing", "requests", 170L)
            .get();
        client().admin().indices().prepareRefresh("ts_counters").get();
    }

    private void registerEmployees() {
        registerDataSource("local_ds", Map.of());
        registerDataset("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"));
    }

    private void createView(String name, String query) {
        assertAcked(
            client().execute(PutViewAction.INSTANCE, new PutViewAction.Request(TIMEOUT, TIMEOUT, new View(name, query))).actionGet(TIMEOUT)
        );
    }

    private void registerEmployeesAlt() {
        registerDataset("employees_alt", "local_ds", csvFixtureAlt.toUri().toString(), Map.of("format", "csv"));
    }

    /** Registers {@code salaries_int} whose {@code salary} column is typed {@code integer}. */
    private void registerSalariesInt() {
        registerDataSource("local_ds", Map.of());
        registerDataset("salaries_int", "local_ds", csvFixtureSalaryInt.toUri().toString(), Map.of("format", "csv"));
    }

    /** Registers {@code salaries_long} whose {@code salary} column is typed {@code long}. */
    private void registerSalariesLong() {
        registerDataSource("local_ds", Map.of());
        registerDataset("salaries_long", "local_ds", csvFixtureSalaryLong.toUri().toString(), Map.of("format", "csv"));
    }

    private static void assertCauseMessageContains(Throwable throwable, String fragment) {
        Throwable cause = throwable;
        while (cause != null && (cause.getMessage() == null || cause.getMessage().contains(fragment) == false)) {
            cause = cause.getCause();
        }
        assertThat("error chain should contain message fragment [" + fragment + "]", cause, org.hamcrest.Matchers.notNullValue());
    }
}
