/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.view.DeleteViewAction;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 * End-to-end coverage for ES|QL views whose body reads a registered dataset.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class FromDatasetViewIT extends AbstractExternalDataSourceIT {

    private Path csvFixture;
    private Path csvFixtureAlt;
    private Path csvFixtureSalaryInt;
    private Path csvFixtureSalaryLong;
    private Path csvFixtureSalaryKeyword;
    private final Set<String> createdViews = new LinkedHashSet<>();

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    @Before
    public void writeFixture() throws IOException {
        csvFixture = createTempFile("dataset-view-fixture-", ".csv");
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
        csvFixtureAlt = createTempFile("dataset-view-fixture-alt-", ".csv");
        Files.writeString(
            csvFixtureAlt,
            String.join(
                "\n",
                "emp_no:integer,first_name:keyword,last_name:keyword,department:keyword,salary:integer",
                "10,Diana,Davis,Engineering,75000",
                "11,Eve,Evans,Sales,65000"
            ) + "\n"
        );
        csvFixtureSalaryInt = createTempFile("dataset-view-salary-int-", ".csv");
        Files.writeString(
            csvFixtureSalaryInt,
            String.join("\n", "emp_no:integer,name:keyword,salary:integer", "1,Alice,50000", "2,Bob,60000") + "\n"
        );
        csvFixtureSalaryLong = createTempFile("dataset-view-salary-long-", ".csv");
        Files.writeString(
            csvFixtureSalaryLong,
            String.join("\n", "emp_no:integer,name:keyword,salary:long", "10,Diana,75000", "11,Eve,65000") + "\n"
        );
        csvFixtureSalaryKeyword = createTempFile("dataset-view-salary-keyword-", ".csv");
        Files.writeString(csvFixtureSalaryKeyword, String.join("\n", "emp_no:integer,name:keyword,salary:keyword", "20,Frank,high") + "\n");
    }

    @After
    public void cleanupViews() {
        if (createdViews.isEmpty() == false) {
            try {
                client().execute(
                    DeleteViewAction.INSTANCE,
                    new DeleteViewAction.Request(TIMEOUT, TIMEOUT, createdViews.toArray(String[]::new))
                ).actionGet(TIMEOUT);
            } catch (ResourceNotFoundException ignored) {
                // not created, or already deleted
            } catch (Exception e) {
                logger.warn("view cleanup {} failed", createdViews, e);
            }
        }
        createdViews.clear();
    }

    // dataset inside view

    public void testDatasetView() {
        registerEmployees();
        createView("employees_view", "FROM employees");
        try (var response = run(syncEsqlQueryRequest("FROM employees_view | SORT emp_no"))) {
            assertColumnNames(response.columns(), List.of("emp_no", "first_name", "last_name", "department", "salary"));
            assertThat(
                getValuesList(response),
                equalTo(
                    List.of(
                        List.of(1, "Alice", "Anderson", "Engineering", 50000),
                        List.of(2, "Bob", "Brown", "Engineering", 60000),
                        List.of(3, "Carol", "Cox", "Sales", 55000)
                    )
                )
            );
        }
    }

    public void testWhereAfterDatasetView() {
        registerEmployees();
        createView("emp_ds_view", "FROM employees");
        try (var response = run(syncEsqlQueryRequest("FROM emp_ds_view | WHERE emp_no > 1 | SORT emp_no | KEEP emp_no, first_name"))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(2, "Bob"), List.of(3, "Carol"))));
        }
    }

    public void testEvalKeepRenameDropAfterDatasetView() {
        registerEmployees();
        createView("emp_ds_view", "FROM employees");
        try (var response = run(syncEsqlQueryRequest("""
            FROM emp_ds_view
            | EVAL doubled = emp_no * 2
            | RENAME first_name AS name
            | DROP last_name
            | KEEP emp_no, name, doubled
            | SORT emp_no
            """))) {
            assertColumnNames(response.columns(), List.of("emp_no", "name", "doubled"));
            assertThat(getValuesList(response), equalTo(List.of(List.of(1, "Alice", 2), List.of(2, "Bob", 4), List.of(3, "Carol", 6))));
        }
    }

    public void testStatsAfterDatasetView() {
        registerEmployees();
        createView("emp_ds_view", "FROM employees");
        try (var response = run(syncEsqlQueryRequest("""
            FROM emp_ds_view
            | STATS c = COUNT(*) BY department
            | KEEP department, c
            | SORT department
            """))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of("Engineering", 2L), List.of("Sales", 1L))));
        }
    }

    public void testInlineStatsAfterDatasetView() {
        registerEmployees();
        createView("emp_ds_view", "FROM employees");
        try (var response = run(syncEsqlQueryRequest("""
            FROM emp_ds_view
            | INLINE STATS c = COUNT(*) BY department
            | KEEP emp_no, department, c
            | SORT emp_no
            """))) {
            assertThat(
                getValuesList(response),
                equalTo(List.of(List.of(1, "Engineering", 2L), List.of(2, "Engineering", 2L), List.of(3, "Sales", 1L)))
            );
        }
    }

    public void testLookupJoinAfterDatasetView() {
        registerEmployees();
        createDepartmentsLookup();
        createView("emp_ds_view", "FROM employees");
        try (var response = run(syncEsqlQueryRequest("""
            FROM emp_ds_view
            | LOOKUP JOIN departments_lookup ON department
            | KEEP emp_no, department, location
            | SORT emp_no
            """))) {
            assertThat(
                getValuesList(response),
                equalTo(
                    List.of(
                        List.of(1, "Engineering", "Mountain View"),
                        List.of(2, "Engineering", "Mountain View"),
                        List.of(3, "Sales", "New York")
                    )
                )
            );
        }
    }

    public void testMatchAndMatchPhraseAfterDatasetView() {
        registerEmployees();
        createView("emp_ds_view", "FROM employees");
        try (var response = run(syncEsqlQueryRequest("""
            FROM emp_ds_view
            | WHERE MATCH(first_name, "Alice")
            | KEEP first_name, last_name
            """))) {
            assertColumnNames(response.columns(), List.of("first_name", "last_name"));
            assertValues(response.values(), List.of(List.of("Alice", "Anderson")));
        }
        try (var response = run(syncEsqlQueryRequest("""
            FROM emp_ds_view
            | WHERE MATCH_PHRASE(first_name, "Alice")
            | KEEP first_name, last_name
            """))) {
            assertColumnNames(response.columns(), List.of("first_name", "last_name"));
            assertValues(response.values(), List.of(List.of("Alice", "Anderson")));
        }
    }

    public void testInSubqueryAfterDatasetView() {
        registerEmployees();
        createView("emp_ds_view", "FROM employees");
        try (var response = run(syncEsqlQueryRequest("""
            FROM emp_ds_view
            | WHERE emp_no IN (FROM employees | WHERE department == "Engineering" | KEEP emp_no)
            | SORT emp_no
            | KEEP emp_no
            """))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(1), List.of(2))));
        }

        try (var response = run(syncEsqlQueryRequest("""
            FROM emp_ds_view
            | WHERE emp_no NOT IN (FROM employees | WHERE department == "Engineering" | KEEP emp_no)
            | SORT emp_no
            | KEEP emp_no
            """))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(3))));
        }
    }

    public void testDatasetViewInsideInSubquery() {
        registerEmployees();
        createView("emp_ds_view", "FROM employees");
        try (var response = run(syncEsqlQueryRequest("""
            FROM employees
            | WHERE department IN (FROM emp_ds_view | KEEP department)
            | SORT emp_no
            | KEEP emp_no
            """))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(1), List.of(2), List.of(3))));
        }
    }

    // dataset with processing commands inside view

    public void testWhereInsideDatasetView() {
        registerEmployees();
        createView("employees_filtered_view", "FROM employees | WHERE emp_no > 1");
        try (var response = run(syncEsqlQueryRequest("FROM employees_filtered_view | SORT emp_no | KEEP emp_no, first_name"))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(2, "Bob"), List.of(3, "Carol"))));
        }
    }

    public void testStatsInsideDatasetView() {
        registerEmployees();
        createView("emp_stats_view", "FROM employees | STATS c = COUNT(*) BY department");
        try (var response = run(syncEsqlQueryRequest("FROM emp_stats_view | KEEP department, c | SORT department"))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of("Engineering", 2L), List.of("Sales", 1L))));
        }
    }

    public void testInlineStatsInsideDatasetView() {
        registerEmployees();
        createView("emp_inline_stats_view", """
            FROM employees
            | INLINE STATS max_salary = MAX(salary)
            | WHERE salary == max_salary
            | KEEP emp_no, salary, max_salary
            """);
        try (var response = run(syncEsqlQueryRequest("FROM emp_inline_stats_view"))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(2, 60000, 60000))));
        }
    }

    public void testLookupJoinInsideDatasetView() {
        registerEmployees();
        createDepartmentsLookup();
        createView("emp_lookup_view", "FROM employees | LOOKUP JOIN departments_lookup ON department | KEEP emp_no, department, location");
        try (var response = run(syncEsqlQueryRequest("FROM emp_lookup_view | SORT emp_no"))) {
            assertThat(
                getValuesList(response),
                equalTo(
                    List.of(
                        List.of(1, "Engineering", "Mountain View"),
                        List.of(2, "Engineering", "Mountain View"),
                        List.of(3, "Sales", "New York")
                    )
                )
            );
        }
    }

    public void testDissectInsideDatasetView() {
        registerEmployees();
        createView("emp_dissect_view", "FROM employees | DISSECT first_name \"%{first_letter}lice\" | KEEP emp_no, first_letter");
        try (var response = run(syncEsqlQueryRequest("FROM emp_dissect_view | SORT emp_no"))) {
            assertColumnNames(response.columns(), List.of("emp_no", "first_letter"));
            assertThat(getValuesList(response), equalTo(List.of(List.of(1, "A"), Arrays.asList(2, null), Arrays.asList(3, null))));
        }
    }

    public void testSortLimitInsideDatasetView() {
        registerEmployees();
        createView("emp_sort_limit_view", "FROM employees | SORT salary DESC | LIMIT 2 | KEEP emp_no, first_name");
        try (var response = run(syncEsqlQueryRequest("FROM emp_sort_limit_view"))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(2, "Bob"), List.of(3, "Carol"))));
        }
    }

    public void testInSubqueryInsideDatasetView() {
        registerEmployees();
        createView("emp_in_view", """
            FROM employees
            | WHERE emp_no IN (FROM employees | WHERE department == "Sales" | KEEP emp_no)
            """);
        try (var response = run(syncEsqlQueryRequest("FROM emp_in_view | KEEP emp_no, first_name"))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(3, "Carol"))));
        }
    }

    public void testEvalKeepRenameDropInsideDatasetView() {
        registerEmployees();
        createView("emp_eval_view", """
            FROM employees
            | EVAL doubled = emp_no * 2
            | RENAME first_name AS name
            | DROP last_name
            | KEEP emp_no, name, doubled
            """);
        try (var response = run(syncEsqlQueryRequest("FROM emp_eval_view | SORT emp_no"))) {
            assertColumnNames(response.columns(), List.of("emp_no", "name", "doubled"));
            assertThat(getValuesList(response), equalTo(List.of(List.of(1, "Alice", 2), List.of(2, "Bob", 4), List.of(3, "Carol", 6))));
        }
    }

    public void testMatchInsideDatasetView() {
        registerEmployees();
        createView("emp_match_view", "FROM employees | WHERE MATCH(first_name, \"Alice\") | KEEP first_name, last_name");
        try (var response = run(syncEsqlQueryRequest("FROM emp_match_view"))) {
            assertColumnNames(response.columns(), List.of("first_name", "last_name"));
            assertValues(response.values(), List.of(List.of("Alice", "Anderson")));
        }
    }

    public void testStatsAfterStatsDatasetView() {
        registerEmployees();
        createView("emp_stats_view", "FROM employees | STATS c = COUNT(*) BY department");
        try (var response = run(syncEsqlQueryRequest("FROM emp_stats_view | STATS total = SUM(c)"))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(3L))));
        }
    }

    // UnionAll subquery inside view over dataset, nested branches

    public void testUnionAllSubqueryInsideDatasetView() {
        registerEmployees();
        registerEmployeesAlt();
        createView("emp_union_view", "FROM (FROM employees), (FROM employees_alt)");
        try (var response = run(syncEsqlQueryRequest("FROM emp_union_view | KEEP emp_no, first_name | SORT emp_no, first_name"))) {
            assertColumnNames(response.columns(), List.of("emp_no", "first_name"));
            assertThat(
                getValuesList(response),
                equalTo(List.of(List.of(1, "Alice"), List.of(2, "Bob"), List.of(3, "Carol"), List.of(10, "Diana"), List.of(11, "Eve")))
            );
        }
    }

    public void testMultipleDatasetViewsWithUnionAllSubquery() {
        registerEmployees();
        registerEmployeesAlt();
        createView("view_eng", """
            FROM
                (FROM employees | WHERE department == "Engineering"),
                (FROM employees_alt | WHERE department == "Engineering")
            """);
        createView("view_sales", """
            FROM
                (FROM employees | WHERE department == "Sales"),
                (FROM employees_alt | WHERE department == "Sales")
            """);
        try (var response = run(syncEsqlQueryRequest("FROM view_eng, view_sales | KEEP emp_no, first_name | SORT emp_no, first_name"))) {
            assertColumnNames(response.columns(), List.of("emp_no", "first_name"));
            assertThat(
                getValuesList(response),
                equalTo(List.of(List.of(1, "Alice"), List.of(2, "Bob"), List.of(3, "Carol"), List.of(10, "Diana"), List.of(11, "Eve")))
            );
        }
    }

    public void testNestedDatasetViews() {
        registerEmployees();
        createView("emp_nested_view_1", "FROM employees");
        createView("emp_nested_view_2", "FROM emp_nested_view_1");
        createView("emp_nested_view_3", "FROM emp_nested_view_2");
        try (var response = run(syncEsqlQueryRequest("FROM emp_nested_view_3 | SORT emp_no | KEEP emp_no, first_name"))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(1, "Alice"), List.of(2, "Bob"), List.of(3, "Carol"))));
        }
    }

    public void testDatasetViewAndRegularIndex() {
        registerEmployees();
        createRealEmployees();
        createView("emp_ds_view", "FROM employees");
        try (
            var response = run(syncEsqlQueryRequest("FROM emp_ds_view, real_employees | KEEP emp_no, first_name | SORT emp_no, first_name"))
        ) {
            assertColumnNames(response.columns(), List.of("emp_no", "first_name"));
            assertThat(
                getValuesList(response),
                equalTo(
                    List.of(
                        List.of(1, "Alice"),
                        List.of(1, "Alice-real"),
                        List.of(2, "Bob"),
                        List.of(3, "Carol"),
                        List.of(3, "Carol-real"),
                        List.of(99, "Zach-real"),
                        List.of(100, "Frank"),
                        List.of(101, "Grace")
                    )
                )
            );
        }
    }

    public void testRegularIndexViewAndDataset() {
        registerEmployees();
        createRealEmployees();
        createView("emp_idx_view", "FROM real_employees");
        try (var response = run(syncEsqlQueryRequest("FROM emp_idx_view, employees | KEEP emp_no, first_name | SORT emp_no, first_name"))) {
            assertColumnNames(response.columns(), List.of("emp_no", "first_name"));
            assertThat(
                getValuesList(response),
                equalTo(
                    List.of(
                        List.of(1, "Alice"),
                        List.of(1, "Alice-real"),
                        List.of(2, "Bob"),
                        List.of(3, "Carol"),
                        List.of(3, "Carol-real"),
                        List.of(99, "Zach-real"),
                        List.of(100, "Frank"),
                        List.of(101, "Grace")
                    )
                )
            );
        }
    }

    public void testDatasetViewAndDataset() {
        registerEmployees();
        createView("emp_ds_view", "FROM employees");
        try (var response = run(syncEsqlQueryRequest("FROM emp_ds_view, employees | KEEP emp_no, first_name | SORT emp_no, first_name"))) {
            assertColumnNames(response.columns(), List.of("emp_no", "first_name"));
            assertThat(
                getValuesList(response),
                equalTo(
                    List.of(
                        List.of(1, "Alice"),
                        List.of(1, "Alice"),
                        List.of(2, "Bob"),
                        List.of(2, "Bob"),
                        List.of(3, "Carol"),
                        List.of(3, "Carol")
                    )
                )
            );
        }
    }

    public void testConflictingTypesResolvedByCastInsideDatasetView() {
        registerSalariesInt();
        registerSalariesLong();
        createView("view_int_cast", "FROM salaries_int | EVAL salary = salary::long");
        createView("view_long", "FROM salaries_long");
        try (var response = run(syncEsqlQueryRequest("FROM view_int_cast, view_long | SORT salary"))) {
            assertColumnNames(response.columns(), List.of("emp_no", "name", "salary"));
            assertThat(
                getValuesList(response),
                equalTo(
                    List.of(
                        List.of(1, "Alice", 50000L),
                        List.of(2, "Bob", 60000L),
                        List.of(11, "Eve", 65000L),
                        List.of(10, "Diana", 75000L)
                    )
                )
            );
        }
    }

    public void testDatasetViewsWithDifferentColumnsFillNulls() {
        registerEmployees();
        registerSalariesInt();
        createView("view_first_name", "FROM employees | KEEP emp_no, first_name");
        createView("view_name", "FROM salaries_int | KEEP emp_no, name");
        try (var response = run(syncEsqlQueryRequest("""
            FROM view_first_name, view_name
            | SORT emp_no, first_name
            | KEEP emp_no, first_name, name
            """))) {
            assertColumnNames(response.columns(), List.of("emp_no", "first_name", "name"));
            assertThat(
                getValuesList(response),
                equalTo(
                    Arrays.asList(
                        Arrays.asList(1, "Alice", null),
                        Arrays.asList(1, null, "Alice"),
                        Arrays.asList(2, "Bob", null),
                        Arrays.asList(2, null, "Bob"),
                        Arrays.asList(3, "Carol", null)
                    )
                )
            );
        }
    }

    // wildcard

    public void testWildcardMatchesViews() {
        registerEmployees();
        registerEmployeesAlt();
        createView("emp_view_a", "FROM employees");
        createView("emp_view_b", "FROM employees_alt");
        try (
            var response = run(
                syncEsqlQueryRequest(
                    "SET wildcards_match_views = true; FROM emp_view_* | KEEP emp_no, first_name | SORT emp_no, first_name"
                )
            )
        ) {
            assertColumnNames(response.columns(), List.of("emp_no", "first_name"));
            assertThat(
                getValuesList(response),
                equalTo(List.of(List.of(1, "Alice"), List.of(2, "Bob"), List.of(3, "Carol"), List.of(10, "Diana"), List.of(11, "Eve")))
            );
        }
    }

    public void testWildcardMatchesDatasetsDoesNotMatchViews() {
        registerEmployees();
        createView("emp_view_a", "FROM employees");
        createView("emp_view_b", "FROM employees");
        try (var response = run(syncEsqlQueryRequest("SET wildcards_match_datasets = true; FROM emp_view_* | STATS c = COUNT(*)"))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(0L))));
        }
    }

    public void testDatasetAndWildcardMatchesView() {
        registerEmployees();
        registerEmployeesAlt();
        createView("emp_view_a", "FROM employees_alt");
        try (
            var response = run(
                syncEsqlQueryRequest(
                    "SET wildcards_match_views = true; FROM employees, emp_view_* | KEEP emp_no, first_name | SORT emp_no, first_name"
                )
            )
        ) {
            assertColumnNames(response.columns(), List.of("emp_no", "first_name"));
            assertThat(
                getValuesList(response),
                equalTo(List.of(List.of(1, "Alice"), List.of(2, "Bob"), List.of(3, "Carol"), List.of(10, "Diana"), List.of(11, "Eve")))
            );
        }
    }

    public void testWildcardMatchesViewWithExclusion() {
        registerEmployees();
        registerEmployeesAlt();
        createView("emp_view_a", "FROM employees");
        createView("emp_view_b", "FROM employees_alt");
        try (
            var response = run(
                syncEsqlQueryRequest(
                    "SET wildcards_match_views = true; FROM emp_view_*,-emp_view_b | KEEP emp_no, first_name | SORT emp_no, first_name"
                )
            )
        ) {
            assertColumnNames(response.columns(), List.of("emp_no", "first_name"));
            assertThat(getValuesList(response), equalTo(List.of(List.of(1, "Alice"), List.of(2, "Bob"), List.of(3, "Carol"))));
        }
    }

    public void testWildcardMatchesViewAndDataset() {
        registerEmployees();
        registerEmployeesAlt();
        createView("employees_view", "FROM employees_alt");
        // A wildcard that matches a view is consumed by view resolution and is not also rewritten to datasets, so only the view's
        // employees_alt rows remain.
        try (var response = run(syncEsqlQueryRequest("""
            SET wildcards_match_views = true;
            SET wildcards_match_datasets = true;
            FROM employees*
            | KEEP emp_no, first_name
            | SORT emp_no, first_name
            """))) {
            assertColumnNames(response.columns(), List.of("emp_no", "first_name"));
            assertThat(
                getValuesList(response),
                equalTo(
                    List.of(
                        List.of(1, "Alice"),
                        List.of(2, "Bob"),
                        List.of(3, "Carol"),
                        List.of(10, "Diana"),
                        List.of(10, "Diana"),
                        List.of(11, "Eve"),
                        List.of(11, "Eve")
                    )
                )
            );
        }
    }

    public void testWildcardMatchesViewAndDatasetWithExclusion() {
        registerEmployees();
        registerEmployeesAlt();
        createView("employees_view", "FROM employees_alt");
        // employees* matches employees_view (2), employees (3), and employees_alt (2). Excluding employees_alt leaves
        // the view plus the employees dataset: 5. Same composition as testWildcardMatchesViewAndDataset.
        try (var response = run(syncEsqlQueryRequest("""
            SET wildcards_match_views = true;
            SET wildcards_match_datasets = true;
            FROM employees*,-employees_alt
            | KEEP emp_no, first_name
            | SORT emp_no, first_name
            """))) {
            assertColumnNames(response.columns(), List.of("emp_no", "first_name"));
            assertThat(
                getValuesList(response),
                equalTo(List.of(List.of(1, "Alice"), List.of(2, "Bob"), List.of(3, "Carol"), List.of(10, "Diana"), List.of(11, "Eve")))
            );
        }
    }

    public void testWildcardInViewBodyDoesNotMatchDatasets() {
        registerEmployees();
        registerEmployeesAlt();
        createView("emp_star_view", "FROM employees*");
        // Default: a wildcard stored in the view body reaches no dataset.
        try (var response = run(syncEsqlQueryRequest("FROM emp_star_view | STATS c = COUNT(*)"))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(0L))));
        }
        // The caller's SET stops at the view boundary (SETTINGS_SCOPED_AT_VIEW_BOUNDARIES): the view produces the
        // same rows whatever the caller's settings say, so the body's wildcard still reaches no dataset.
        try (var response = run(syncEsqlQueryRequest("SET wildcards_match_datasets = true; FROM emp_star_view | STATS c = COUNT(*)"))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(0L))));
        }
        // The same SET on an outer wildcard reaches both datasets.
        try (
            var response = run(
                syncEsqlQueryRequest(
                    "SET wildcards_match_datasets = true; FROM employees* | KEEP emp_no, first_name | SORT emp_no, first_name"
                )
            )
        ) {
            assertColumnNames(response.columns(), List.of("emp_no", "first_name"));
            assertThat(
                getValuesList(response),
                equalTo(List.of(List.of(1, "Alice"), List.of(2, "Bob"), List.of(3, "Carol"), List.of(10, "Diana"), List.of(11, "Eve")))
            );
        }
    }

    /**
     * A pass-through view next to an index in the same FROM: the view keeps a boundary of its own instead of merging
     * into the caller's relation, so the caller's SET stops at it and the wildcard inside the body matches no dataset.
     */
    public void testWildcardInPassThroughViewBodyBesideIndexDoesNotMatchDatasets() {
        registerEmployees();
        registerEmployeesAlt();
        createRealEmployees();
        createView("emp_star_view", "FROM employees*");
        // Default: only real_employees' 5 rows; the view's wildcard reaches no dataset.
        try (var response = run(syncEsqlQueryRequest("FROM real_employees, emp_star_view | STATS c = COUNT(*)"))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(5L))));
        }
        // The caller's SET stops at the view boundary, so still only real_employees' rows, not the datasets' 5 more.
        try (
            var response = run(
                syncEsqlQueryRequest("SET wildcards_match_datasets = true; FROM real_employees, emp_star_view | STATS c = COUNT(*)")
            )
        ) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(5L))));
        }
    }

    /**
     * A view's own {@code SET wildcards_match_datasets} opens wildcards in that definition only. It does not leak into
     * a nested view, and a nested view that SETs the flag does not need the enclosing view to.
     */
    public void testWildcardMatchDatasetsSetInsideViewAppliesToThatBodyOnly() {
        registerEmployees();
        registerEmployeesAlt();
        createView("emp_star_on", """
            SET wildcards_match_datasets = true;
            FROM employees*
            """);
        try (var response = run(syncEsqlQueryRequest("FROM emp_star_on | KEEP emp_no | SORT emp_no"))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(1), List.of(2), List.of(3), List.of(10), List.of(11))));
        }

        createView("inner_plain", "FROM employees*");
        createView("outer_on", """
            SET wildcards_match_datasets = true;
            FROM inner_plain
            """);
        try (var response = run(syncEsqlQueryRequest("FROM outer_on | STATS c = COUNT(*)"))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(0L))));
        }

        createView("inner_on", """
            SET wildcards_match_datasets = true;
            FROM employees*
            """);
        createView("outer_plain", "FROM inner_on");
        try (var response = run(syncEsqlQueryRequest("FROM outer_plain | KEEP emp_no | SORT emp_no"))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(1), List.of(2), List.of(3), List.of(10), List.of(11))));
        }
    }

    /**
     * A caller who knows a view's contents may expect their {@code SET wildcards_match_datasets=true} to reach the view's
     * wildcards. When a view's wildcard would then have matched datasets, the response says so, naming the view and the datasets.
     */
    public void testWildcardsMatchDatasetsWarnsWhenViewBoundaryStopsIt() throws Exception {
        registerEmployees();
        registerEmployeesAlt();
        createView("emp_star_view", "FROM employees*");
        createView("emp_mixed_view", "FROM employees*, employees_alt");
        createView("emp_exact_view", "FROM employees, employees_alt");
        createView("emp_star_on", """
            SET wildcards_match_datasets = true;
            FROM employees*
            """);
        createView("inner_plain", "FROM employees*");
        createView("outer_on", """
            SET wildcards_match_datasets = true;
            FROM inner_plain
            """);

        assertThat(
            warningHeaders("SET wildcards_match_datasets = true; FROM emp_star_view"),
            hasItem(wildcardsStoppedWarning("emp_star_view", "datasets [employees], [employees_alt]"))
        );
        // A dataset the view also names exactly is read anyway, so only the other one is missed.
        assertThat(
            warningHeaders("SET wildcards_match_datasets = true; FROM emp_mixed_view"),
            hasItem(wildcardsStoppedWarning("emp_mixed_view", "dataset [employees]"))
        );
        // The enclosing view's SET stops at the nested view in the same way, and the warning names the view that SETs it.
        assertThat(
            warningHeaders("FROM outer_on"),
            hasItem(wildcardsStoppedWarning("outer_on", "inner_plain", "datasets [employees], [employees_alt]"))
        );

        for (String query : List.of(
            // The caller did not SET it, so nothing would have matched.
            "FROM emp_star_view",
            // The view SETs it itself.
            "SET wildcards_match_datasets = true; FROM emp_star_on",
            // The view names the datasets it reads exactly.
            "SET wildcards_match_datasets = true; FROM emp_exact_view"
        )) {
            assertThat(query, warningHeaders(query), not(hasItem(containsString("does not affect the contents of view"))));
        }
    }

    private static Matcher<String> wildcardsStoppedWarning(String viewName, String matched) {
        return wildcardsStoppedWarning(null, viewName, matched);
    }

    private static Matcher<String> wildcardsStoppedWarning(@Nullable String settingView, String viewName, String matched) {
        return equalTo(
            "SET wildcards_match_datasets=true"
                + (settingView == null ? "" : " in view [" + settingView + "]")
                + " does not affect the contents of view ["
                + viewName
                + "], so its wildcards did not match "
                + matched
                + "; use that setting inside the view definition if you wish to change the view behaviour"
        );
    }

    /**
     * {@code TS} never reads datasets, so a {@code SET wildcards_match_datasets=true} in the view's definition would only make
     * every query on it fail.
     */
    public void testWildcardsMatchDatasetsDoesNotWarnForTimeSeriesViewBody() throws Exception {
        registerEmployees();
        registerEmployeesAlt();
        Settings settings = Settings.builder()
            .put("mode", "time_series")
            .putList("routing_path", List.of("department"))
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .build();
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("employees_ts")
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
        ensureGreen("employees_ts");
        client().prepareIndex("employees_ts")
            .setSource("@timestamp", "2025-01-01T00:00:00Z", "department", "Sales", "cpu", 0.5)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        createView("emp_ts_view", "TS employees* | STATS m = MAX(MAX_OVER_TIME(cpu))");

        String query = "SET wildcards_match_datasets = true; FROM emp_ts_view";
        try (var response = run(syncEsqlQueryRequest(query))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(0.5))));
        }
        assertThat(warningHeaders(query), not(hasItem(containsString("does not affect the contents of view"))));
    }

    /** The response {@code Warning} headers of {@code query}, decoded. */
    private List<String> warningHeaders(String query) throws InterruptedException {
        List<String> warnings = new CopyOnWriteArrayList<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        client().execute(
            EsqlQueryAction.INSTANCE,
            syncEsqlQueryRequest(query),
            new LatchedActionListener<>(
                ActionListener.wrap(
                    response -> internalCluster().getInstance(TransportService.class)
                        .getThreadPool()
                        .getThreadContext()
                        .getResponseHeaders()
                        .getOrDefault("Warning", List.of())
                        .stream()
                        .map(w -> HeaderWarning.decodeAndUnescape(HeaderWarning.extractWarningValueFromWarningHeader(w, false)))
                        .forEach(warnings::add),
                    failure::set
                ),
                latch
            )
        );
        assertTrue("query did not complete", latch.await(30, TimeUnit.SECONDS));
        if (failure.get() != null) {
            throw new AssertionError("query failed: " + query, failure.get());
        }
        return warnings;
    }

    /**
     * The caller's SET stops at the outer view, whose body has no wildcard of its own. The nested view's wildcard is the nested
     * view's business, so adding the SET to the outer view would change nothing and nothing is reported.
     */
    public void testWildcardInNestedViewBodyDoesNotMatchDatasets() throws Exception {
        registerEmployees();
        registerEmployeesAlt();
        createView("emp_star_view", "FROM employees*");
        createView("emp_nested_star_view", "FROM emp_star_view");
        String query = "SET wildcards_match_datasets = true; FROM emp_nested_star_view | STATS c = COUNT(*)";
        try (var response = run(syncEsqlQueryRequest(query))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(0L))));
        }
        assertThat(warningHeaders(query), not(hasItem(containsString("does not affect the contents of view"))));
    }

    /**
     * Exact names inside a view body are not what the setting controls: the exact dataset is read, the wildcard still is not.
     */
    public void testWildcardAndExactDatasetInViewBody() {
        registerEmployees();
        registerEmployeesAlt();
        createView("emp_mixed_view", "FROM employees*, employees_alt");
        for (String set : List.of("", "SET wildcards_match_datasets = true; ")) {
            try (var response = run(syncEsqlQueryRequest(set + "FROM emp_mixed_view | KEEP emp_no | SORT emp_no"))) {
                assertThat(getValuesList(response), equalTo(List.of(List.of(10), List.of(11))));
            }
        }
    }

    public void testWildcardInNonPassThroughViewBodyDoesNotMatchDatasets() {
        registerEmployees();
        registerEmployeesAlt();
        createView("emp_count_star_view", "FROM employees* | STATS c = COUNT(*)");
        for (String set : List.of("", "SET wildcards_match_datasets = true; ")) {
            try (var response = run(syncEsqlQueryRequest(set + "FROM emp_count_star_view"))) {
                assertThat(getValuesList(response), equalTo(List.of(List.of(0L))));
            }
        }
    }

    public void testWildcardInSubqueryInsideViewBodyDoesNotMatchDatasets() {
        registerEmployees();
        registerEmployeesAlt();
        createView("emp_sub_view", "FROM (FROM employees*), (FROM employees)");
        try (
            var response = run(syncEsqlQueryRequest("SET wildcards_match_datasets = true; FROM emp_sub_view | KEEP emp_no | SORT emp_no"))
        ) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(1), List.of(2), List.of(3))));
        }
    }

    public void testWildcardInViewBodyWithForkDoesNotMatchDatasets() {
        registerEmployees();
        registerEmployeesAlt();
        createView("emp_fork_star_view", "FROM employees*, employees | FORK (WHERE emp_no <= 2) (WHERE emp_no > 2)");
        try (var response = run(syncEsqlQueryRequest("""
            SET wildcards_match_datasets = true; FROM emp_fork_star_view | STATS c = COUNT(*) BY _fork | KEEP _fork, c | SORT _fork"""))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of("fork1", 2L), List.of("fork2", 1L))));
        }
    }

    /**
     * A subquery the user writes is not a view: it keeps the caller's setting.
     */
    public void testWildcardInUserSubqueryMatchesDatasets() {
        registerEmployees();
        registerEmployeesAlt();
        try (
            var response = run(
                syncEsqlQueryRequest("SET wildcards_match_datasets = true; FROM (FROM employees*) | KEEP emp_no | SORT emp_no")
            )
        ) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(1), List.of(2), List.of(3), List.of(10), List.of(11))));
        }
    }

    public void testWildcardMatchesDatasetsIgnoresMatchingView() {
        registerEmployees();
        registerEmployeesAlt();
        createView("employees_view", "FROM employees_alt");
        // wildcards_match_views defaults to false, so employees* reaches the two datasets and not the view.
        try (
            var response = run(
                syncEsqlQueryRequest(
                    "SET wildcards_match_datasets = true; FROM employees* | KEEP emp_no, first_name | SORT emp_no, first_name"
                )
            )
        ) {
            assertColumnNames(response.columns(), List.of("emp_no", "first_name"));
            assertThat(
                getValuesList(response),
                equalTo(List.of(List.of(1, "Alice"), List.of(2, "Bob"), List.of(3, "Carol"), List.of(10, "Diana"), List.of(11, "Eve")))
            );
        }
    }

    // Moved from FromDatasetIT

    public void testViewOverMappedDatasetPreservesCoercion() throws IOException {
        Path mappedCsv = createTempFile("dataset-view-mapped-", ".csv");
        Files.writeString(mappedCsv, "val:integer\n100\n");
        LinkedHashMap<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("val", new DatasetFieldMapping("long", null));
        registerNonStrictDataset("mapped_ds_for_view", mappedCsv.toUri().toString(), properties, Map.of("format", "csv"));
        createView("mapped_dataset_view", "FROM mapped_ds_for_view");
        try (var response = run(syncEsqlQueryRequest("FROM mapped_dataset_view | KEEP val | LIMIT 1"))) {
            assertThat("declared type must survive view inlining", response.columns().get(0).outputType(), equalTo("long"));
            assertThat(getValuesList(response).get(0).get(0), equalTo(100L));
        }
    }

    // dataset view with fork, moved from FromDatasetIT and FromDatasetSubqueryIT

    public void testForkOverCompactedDatasetViews() {
        registerEmployees();
        registerEmployeesAlt();
        createView("fork_dataset_view", "FROM employees, employees_alt");
        createView("fork_dataset_view_a", "FROM employees");
        createView("fork_dataset_view_b", "FROM employees_alt");

        for (String source : List.of(
            "fork_dataset_view",
            "fork_dataset_view_a, employees_alt",
            "fork_dataset_view_a, fork_dataset_view_b"
        )) {
            String query = "FROM " + source + """
                 | FORK
                     (STATS count = COUNT(*))
                     (WHERE emp_no >= 10 | STATS count = COUNT(*))
                 | KEEP _fork, count
                 | SORT _fork
                """;
            try (var response = run(syncEsqlQueryRequest(query))) {
                assertColumnNames(response.columns(), List.of("_fork", "count"));
                assertThat(getValuesList(response), equalTo(List.of(List.of("fork1", 5L), List.of("fork2", 2L))));
            }
        }

        try (var response = run(syncEsqlQueryRequest("""
            FROM fork_dataset_view, employees
             | FORK
                 (STATS count = COUNT(*))
                 (WHERE emp_no >= 10 | STATS count = COUNT(*))
             | KEEP _fork, count
             | SORT _fork
            """))) {
            assertColumnNames(response.columns(), List.of("_fork", "count"));
            assertThat(getValuesList(response), equalTo(List.of(List.of("fork1", 8L), List.of("fork2", 2L))));
        }
    }

    public void testForkAfterDatasetView() {
        registerEmployees();
        createView("emp_ds_view", "FROM employees");
        try (var response = run(syncEsqlQueryRequest("""
            FROM emp_ds_view
            | FORK (WHERE emp_no <= 2) (WHERE emp_no > 2)
            | KEEP _fork, emp_no
            | SORT _fork, emp_no
            """))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of("fork1", 1), List.of("fork1", 2), List.of("fork2", 3))));
        }
    }

    public void testForkInsideDatasetView() {
        registerEmployees();
        createView("emp_fork_view", "FROM employees | FORK (WHERE emp_no <= 2) (WHERE emp_no > 2)");
        try (var response = run(syncEsqlQueryRequest("""
            FROM emp_fork_view
            | STATS c = COUNT(*) BY _fork
            | KEEP _fork, c
            | SORT _fork
            """))) {
            assertColumnNames(response.columns(), List.of("_fork", "c"));
            assertThat(getValuesList(response), equalTo(List.of(List.of("fork1", 2L), List.of("fork2", 1L))));
        }
    }

    public void testForkAfterSubqueryDatasetView() {
        registerEmployees();
        registerEmployeesAlt();
        createView("emp_alt_view", "FROM employees_alt");
        try (var response = run(syncEsqlQueryRequest("""
            FROM (FROM employees), emp_alt_view
            | FORK (WHERE emp_no < 10) (WHERE emp_no >= 10)
            | STATS c = COUNT(*) BY _fork
            | KEEP _fork, c
            | SORT _fork
            """))) {
            assertColumnNames(response.columns(), List.of("_fork", "c"));
            assertThat(getValuesList(response), equalTo(List.of(List.of("fork1", 3L), List.of("fork2", 2L))));
        }
    }

    public void testForkInsideSubqueryInsideDatasetView() {
        registerEmployees();
        registerEmployeesAlt();
        // _fork only exists on the first branch, so the second branch's rows get a null _fork.
        createView("emp_subquery_fork_view", """
            FROM (FROM employees | FORK (WHERE emp_no <= 2) (WHERE emp_no > 2)),
                 (FROM employees_alt)
            """);
        try (var response = run(syncEsqlQueryRequest("""
            FROM emp_subquery_fork_view
            | STATS c = COUNT(*) BY _fork
            | KEEP _fork, c
            | SORT _fork
            """))) {
            assertColumnNames(response.columns(), List.of("_fork", "c"));
            assertThat(getValuesList(response), equalTo(List.of(List.of("fork1", 2L), List.of("fork2", 1L), Arrays.asList(null, 2L))));
        }
    }

    public void testViewReferencingForkDatasetInSubquery() {
        registerEmployees();
        registerEmployeesAlt();
        createView("emp_fork_view", "FROM employees | FORK (WHERE emp_no <= 2) (WHERE emp_no > 2)");
        try (var response = run(syncEsqlQueryRequest("""
            FROM (FROM emp_fork_view),
                 (FROM employees_alt | WHERE emp_no == 10 | EVAL _fork = "fork1")
            | STATS c = COUNT(*) BY _fork
            | KEEP _fork, c
            | SORT _fork
            """))) {
            assertColumnNames(response.columns(), List.of("_fork", "c"));
            assertThat(getValuesList(response), equalTo(List.of(List.of("fork1", 3L), List.of("fork2", 1L))));
        }
    }

    // metadata

    public void testClassAndNameOnDatasetView() {
        registerEmployees();
        createView("emp_ds_view", "FROM employees");
        try (var response = run(syncEsqlQueryRequest("FROM emp_ds_view METADATA _class, _name | KEEP _class, _name | LIMIT 1"))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of("view", "emp_ds_view"))));
        }
        try (var response = run(syncEsqlQueryRequest("FROM emp_ds_view METADATA _class | KEEP _class | LIMIT 1"))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of("view"))));
        }
    }

    public void testClassAndNameOnDatasetViewAndDataset() {
        registerEmployees();
        createView("emp_ds_view", "FROM employees");
        try (var response = run(syncEsqlQueryRequest("""
            FROM emp_ds_view, employees METADATA _class, _name
            | KEEP emp_no, _class, _name
            | SORT emp_no, _class
            """))) {
            assertColumnNames(response.columns(), List.of("emp_no", "_class", "_name"));
            assertThat(
                getValuesList(response),
                equalTo(
                    List.of(
                        List.of(1, "dataset", "employees"),
                        List.of(1, "view", "emp_ds_view"),
                        List.of(2, "dataset", "employees"),
                        List.of(2, "view", "emp_ds_view"),
                        List.of(3, "dataset", "employees"),
                        List.of(3, "view", "emp_ds_view")
                    )
                )
            );
        }
    }

    public void testClassAndNameInViewBody() {
        registerEmployees();
        createView("emp_class_name_view", "FROM employees METADATA _class, _name");
        try (var response = run(syncEsqlQueryRequest("FROM emp_class_name_view | KEEP _class, _name | LIMIT 1"))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of("dataset", "employees"))));
        }
    }

    public void testFileMetadataThroughViewBody() {
        registerEmployees();
        createView("emp_file_meta_view", "FROM employees METADATA _index, _file.path, _file.name");
        String filePath = csvFixture.toUri().toString();
        String fileName = csvFixture.getFileName().toString();
        List<List<Object>> expected = List.of(
            Arrays.asList(1, "Alice", "Anderson", "Engineering", 50000, null, filePath, fileName),
            Arrays.asList(2, "Bob", "Brown", "Engineering", 60000, null, filePath, fileName),
            Arrays.asList(3, "Carol", "Cox", "Sales", 55000, null, filePath, fileName)
        );
        for (String query : List.of("FROM emp_file_meta_view | SORT emp_no", "FROM (FROM emp_file_meta_view) | SORT emp_no")) {
            try (var response = run(syncEsqlQueryRequest(query))) {
                assertColumnNames(
                    response.columns(),
                    List.of("emp_no", "first_name", "last_name", "department", "salary", "_index", "_file.path", "_file.name")
                );
                assertThat(getValuesList(response), equalTo(expected));
            }
        }
    }

    // request filter

    public void testRequestFilterFieldExistsOnDatasetView() {
        registerEmployees();
        createView("emp_ds_view", "FROM employees");

        var term = syncEsqlQueryRequest("FROM emp_ds_view | SORT emp_no | KEEP emp_no, first_name");
        term.filter(QueryBuilders.termQuery("emp_no", 2));
        try (var response = run(term)) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(2, "Bob"))));
        }

        var range = syncEsqlQueryRequest("FROM emp_ds_view | SORT emp_no | KEEP emp_no, first_name");
        range.filter(QueryBuilders.rangeQuery("emp_no").gte(2));
        try (var response = run(range)) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(2, "Bob"), List.of(3, "Carol"))));
        }

        var exists = syncEsqlQueryRequest("FROM emp_ds_view | SORT emp_no | KEEP emp_no");
        exists.filter(QueryBuilders.existsQuery("first_name"));
        try (var response = run(exists)) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(1), List.of(2), List.of(3))));
        }
    }

    public void testRequestFilterFieldMissingOnDatasetView() {
        registerEmployees();
        createView("emp_ds_view", "FROM employees");

        var mustMissing = syncEsqlQueryRequest("FROM emp_ds_view | SORT emp_no");
        mustMissing.filter(QueryBuilders.termQuery("nonexistent", "x"));
        try (var response = run(mustMissing)) {
            assertThat(getValuesList(response), hasSize(0));
        }

        var mustNotMissing = syncEsqlQueryRequest("FROM emp_ds_view | SORT emp_no");
        mustNotMissing.filter(QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("nonexistent", "x")));
        try (var response = run(mustNotMissing)) {
            assertColumnNames(response.columns(), List.of("emp_no", "first_name", "last_name", "department", "salary"));
            assertThat(
                getValuesList(response),
                equalTo(
                    List.of(
                        List.of(1, "Alice", "Anderson", "Engineering", 50000),
                        List.of(2, "Bob", "Brown", "Engineering", 60000),
                        List.of(3, "Carol", "Cox", "Sales", 55000)
                    )
                )
            );
        }
    }

    /**
     * A request filter on a field a view computes with {@code STATS} over a dataset is applied to the view's
     * <em>output</em>, not pushed into the dataset read: a {@code range} on the count selects the bucket after
     * aggregation rather than filtering the dataset rows first. The dataset rewriter used to also wrap the dataset leaf
     * inside the view body, where {@code c} does not exist, so the filter bound to NULL there and dropped every row.
     */
    public void testRequestFilterOnStatsInsideDatasetView() {
        registerEmployees();
        createView("emp_stats_view", "FROM employees | STATS c = COUNT(*) BY department");

        // A source-field filter on the view output selects the same bucket it would have selected at the source.
        var sourceField = syncEsqlQueryRequest("FROM emp_stats_view | KEEP department, c | SORT department");
        sourceField.filter(QueryBuilders.termQuery("department", "Engineering"));
        try (var response = run(sourceField)) {
            assertThat(getValuesList(response), equalTo(List.of(List.of("Engineering", 2L))));
        }

        // A computed view-output field only exists above the view, so the filter must bind there.
        var computed = syncEsqlQueryRequest("FROM emp_stats_view | KEEP department, c | SORT department");
        computed.filter(QueryBuilders.rangeQuery("c").gte(2));
        try (var response = run(computed)) {
            assertColumnNames(response.columns(), List.of("department", "c"));
            assertThat(getValuesList(response), equalTo(List.of(List.of("Engineering", 2L))));
        }
    }

    /**
     * Same as {@link #testRequestFilterOnStatsInsideDatasetView} for a field computed by {@code EVAL}: the filter on
     * {@code doubled} binds to the view output, where the column exists, instead of to the dataset read inside the view.
     */
    public void testRequestFilterOnEvalInsideDatasetView() {
        registerEmployees();
        createView("emp_eval_view", "FROM employees | EVAL doubled = emp_no * 2");

        var sourceField = syncEsqlQueryRequest("FROM emp_eval_view | KEEP emp_no, doubled | SORT emp_no");
        sourceField.filter(QueryBuilders.termQuery("emp_no", 1));
        try (var response = run(sourceField)) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(1, 2))));
        }

        var computed = syncEsqlQueryRequest("FROM emp_eval_view | KEEP emp_no, doubled | SORT emp_no");
        computed.filter(QueryBuilders.termQuery("doubled", 2));
        try (var response = run(computed)) {
            assertColumnNames(response.columns(), List.of("emp_no", "doubled"));
            assertThat(getValuesList(response), equalTo(List.of(List.of(1, 2))));
        }
    }

    /**
     * A view over a dataset nested inside a user-written subquery branch is still filtered. The outer union has a view
     * branch ({@code emp_eval_view}) and a literal subquery branch that itself reads {@code emp_filtered_view}; the
     * request filter must reach both views' output. Without that, the dataset read inside the nested view is skipped by
     * the dataset rewriter (it sits under a view branch) and the nested view is skipped by the view rewriter (it sits
     * below the outer union), so its rows leak through unfiltered.
     */
    public void testRequestFilterOnDatasetViewNestedInSubquery() {
        registerEmployees();
        createView("emp_eval_view", "FROM employees | EVAL doubled = emp_no * 2");
        createView("emp_filtered_view", "FROM employees | WHERE emp_no > 1");

        var request = syncEsqlQueryRequest("FROM emp_eval_view, (FROM emp_filtered_view | WHERE emp_no > 0) | KEEP emp_no | SORT emp_no");
        request.filter(QueryBuilders.termQuery("emp_no", 2));
        try (var response = run(request)) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(2), List.of(2))));
        }

        // The same shape filtered on a field only the nested view computes: the filter must bind against that view's
        // output, not the dataset read inside it. The passthrough view binds c to NULL and contributes no rows.
        createView("emp_ds_view", "FROM employees");
        createView("emp_stats_view", "FROM employees | STATS c = COUNT(*) BY department");
        request = syncEsqlQueryRequest("FROM emp_ds_view, (FROM emp_stats_view | WHERE c > 0) | KEEP emp_no, c | SORT c");
        request.filter(QueryBuilders.rangeQuery("c").gte(2));
        try (var response = run(request)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(rows.get(0).get(0), nullValue());
            assertThat(rows.get(0).get(1), equalTo(2L));
        }
    }

    public void testRequestFilterOnUnionSubqueryInsideDatasetView() {
        registerEmployees();
        registerEmployeesAlt();
        createView("emp_union_view", "FROM (FROM employees), (FROM employees_alt)");
        var request = syncEsqlQueryRequest("FROM emp_union_view | KEEP emp_no | SORT emp_no");
        request.filter(QueryBuilders.rangeQuery("emp_no").gte(10));
        try (var response = run(request)) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(10), List.of(11))));
        }
    }

    public void testRequestFilterOnForkAfterSubqueryDatasetView() {
        registerEmployees();
        registerEmployeesAlt();
        createView("emp_alt_view", "FROM employees_alt");
        var request = syncEsqlQueryRequest("""
            FROM (FROM employees), emp_alt_view
            | FORK (WHERE emp_no < 10) (WHERE emp_no >= 10)
            | STATS c = COUNT(*) BY _fork
            | KEEP _fork, c
            | SORT _fork
            """);
        request.filter(QueryBuilders.rangeQuery("emp_no").gte(2));
        try (var response = run(request)) {
            assertColumnNames(response.columns(), List.of("_fork", "c"));
            assertThat(getValuesList(response), equalTo(List.of(List.of("fork1", 2L), List.of("fork2", 2L))));
        }
    }

    public void testRequestFilterOnMixedDatasetViewDatasetRegularIndex() {
        registerEmployees();
        registerEmployeesAlt();
        createRealEmployees();
        createView("emp_fork_view", "FROM employees | FORK (WHERE emp_no <= 2) (WHERE emp_no > 2)");
        var request = syncEsqlQueryRequest("""
            FROM emp_fork_view, (FROM employees_alt), real_employees
            | KEEP emp_no
            | SORT emp_no
            """);
        request.filter(QueryBuilders.rangeQuery("emp_no").gte(3));
        try (var response = run(request)) {
            assertThat(
                getValuesList(response),
                equalTo(List.of(List.of(3), List.of(3), List.of(10), List.of(11), List.of(99), List.of(100), List.of(101)))
            );
        }
    }

    // negative tests

    public void testDatasetViewRejectedAsTSSource() {
        registerEmployees();
        createView("emp_ds_view", "FROM employees");
        Exception ex = expectThrows(Exception.class, () -> run(syncEsqlQueryRequest("TS emp_ds_view | LIMIT 1")));
        // View resolution rejects the view name before DatasetRewriter can see the inner dataset.
        assertCauseMessageContains(ex, "Views are not supported in TS command");
    }

    public void testDatasetViewRejectedAsRHSOfLookupJoin() {
        registerEmployees();
        createRealEmployees();
        createView("emp_ds_view", "FROM employees");
        Exception ex = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM real_employees | LOOKUP JOIN emp_ds_view ON emp_no | LIMIT 1"))
        );
        // LOOKUP JOIN does not expand a view target, so the dataset rewriter never fires; the
        // view name is resolved as a missing index instead.
        assertCauseMessageContains(ex, "Unknown index [emp_ds_view]");
    }

    public void testDatasetViewRejectedByKQL() {
        registerEmployees();
        createView("emp_ds_view", "FROM employees");
        Exception ex = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM emp_ds_view | WHERE KQL(\"first_name: Alice\")"))
        );
        assertCauseMessageContains(
            ex,
            "[KQL] function is not supported on federated data sources [employees]; it requires an index. "
                + "Use MATCH(field, \"term\") for full-text search on non-indexed data."
        );
    }

    public void testDatasetViewRejectedByQSTR() {
        registerEmployees();
        createView("emp_ds_view", "FROM employees");
        Exception ex = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM emp_ds_view | WHERE QSTR(\"first_name: Alice\")"))
        );
        assertCauseMessageContains(
            ex,
            "[QSTR] function is not supported on federated data sources [employees]; it requires an index. "
                + "Use MATCH(field, \"term\") for full-text search on non-indexed data."
        );
    }

    public void testMissingDatasetInsideViewIsUnknownIndex() {
        registerEmployees();
        createView("emp_missing_view", "FROM missing_employees");
        Exception ex = expectThrows(Exception.class, () -> run(syncEsqlQueryRequest("FROM emp_missing_view | LIMIT 1")));
        assertCauseMessageContains(ex, "Unknown index [missing_employees]");
    }

    public void testConflictingTypesAcrossDatasetViewsRejected() {
        registerSalariesInt();
        registerSalariesLong();
        createView("view_int", "FROM salaries_int");
        createView("view_long", "FROM salaries_long");
        Exception ex = expectThrows(Exception.class, () -> run(syncEsqlQueryRequest("FROM view_int, view_long | SORT salary")));
        assertCauseMessageContains(ex, "Column [salary] has conflicting data types in subqueries");
    }

    public void testConflictingKeywordAndNumericTypesAcrossDatasetViewsRejected() {
        registerSalariesInt();
        registerSalariesKeyword();
        createView("view_int", "FROM salaries_int");
        createView("view_keyword", "FROM salaries_keyword");
        Exception ex = expectThrows(Exception.class, () -> run(syncEsqlQueryRequest("FROM view_int, view_keyword | SORT salary")));
        assertCauseMessageContains(ex, "Column [salary] has conflicting data types in subqueries");
    }

    public void testDatasetViewExceedsMaxBranchCount() {
        assumeTrue("requires query pragmas", canUseQueryPragmas());
        registerEmployees();
        registerEmployeesAlt();
        for (int i = 1; i <= 5; i++) {
            createView("branch_view_" + i, "FROM (FROM employees), (FROM employees_alt)");
        }
        var tightCount = new QueryPragmas(Settings.builder().put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), 8).build());
        expectThrows(
            VerificationException.class,
            containsString("exceeding the limit of 8"),
            () -> run(
                syncEsqlQueryRequest("FROM branch_view_1, branch_view_2, branch_view_3, branch_view_4, branch_view_5 | STATS c = COUNT(*)")
                    .pragmas(tightCount)
            ).close()
        );
    }

    public void testDatasetViewExceedsMaxBranchLevel() {
        assumeTrue("requires query pragmas", canUseQueryPragmas());
        registerEmployees();
        registerEmployeesAlt();
        createView("emp_eval_union_view", "FROM (FROM employees), (FROM employees_alt) | EVAL tag = 1");
        var tightLevel = new QueryPragmas(Settings.builder().put(QueryPragmas.MAX_BRANCH_LEVEL.getKey(), 1).build());
        expectThrows(
            VerificationException.class,
            containsString("nested union levels"),
            () -> run(syncEsqlQueryRequest("FROM emp_eval_union_view, employees | KEEP emp_no").pragmas(tightLevel)).close()
        );
    }

    // helpers

    private void registerEmployees() {
        registerDataSource("local_ds", Map.of());
        registerDataset("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"));
    }

    private void registerEmployeesAlt() {
        registerDataset("employees_alt", "local_ds", csvFixtureAlt.toUri().toString(), Map.of("format", "csv"));
    }

    private void registerSalariesInt() {
        registerDataSource("local_ds", Map.of());
        registerDataset("salaries_int", "local_ds", csvFixtureSalaryInt.toUri().toString(), Map.of("format", "csv"));
    }

    private void registerSalariesLong() {
        registerDataset("salaries_long", "local_ds", csvFixtureSalaryLong.toUri().toString(), Map.of("format", "csv"));
    }

    private void registerSalariesKeyword() {
        registerDataSource("local_ds", Map.of());
        registerDataset("salaries_keyword", "local_ds", csvFixtureSalaryKeyword.toUri().toString(), Map.of("format", "csv"));
    }

    private void createView(String name, String query) {
        assertAcked(
            client().execute(PutViewAction.INSTANCE, new PutViewAction.Request(TIMEOUT, TIMEOUT, new View(name, query))).actionGet(TIMEOUT)
        );
        createdViews.add(name);
    }

    private void createDepartmentsLookup() {
        Settings lookupSettings = Settings.builder().put("index.number_of_shards", 1).put("index.mode", "lookup").build();
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("departments_lookup")
                .setSettings(lookupSettings)
                .setMapping("department", "type=keyword", "location", "type=keyword")
        );
        BulkResponse bulk = client().prepareBulk()
            .add(client().prepareIndex("departments_lookup").setSource("department", "Engineering", "location", "Mountain View"))
            .add(client().prepareIndex("departments_lookup").setSource("department", "Sales", "location", "New York"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        assertFalse(bulk.buildFailureMessage(), bulk.hasFailures());
    }

    private void createRealEmployees() {
        assertAcked(
            client().admin().indices().prepareCreate("real_employees").setMapping("emp_no", "type=integer", "first_name", "type=keyword")
        );
        ensureGreen("real_employees");
        BulkResponse bulk = client().prepareBulk()
            .add(client().prepareIndex("real_employees").setSource("emp_no", 1, "first_name", "Alice-real"))
            .add(client().prepareIndex("real_employees").setSource("emp_no", 3, "first_name", "Carol-real"))
            .add(client().prepareIndex("real_employees").setSource("emp_no", 99, "first_name", "Zach-real"))
            .add(client().prepareIndex("real_employees").setSource("emp_no", 100, "first_name", "Frank"))
            .add(client().prepareIndex("real_employees").setSource("emp_no", 101, "first_name", "Grace"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        assertFalse(bulk.buildFailureMessage(), bulk.hasFailures());
    }

    private static void assertCauseMessageContains(Throwable throwable, String fragment) {
        StringBuilder chain = new StringBuilder();
        Throwable cause = throwable;
        while (cause != null) {
            if (cause.getMessage() != null) {
                if (chain.isEmpty() == false) {
                    chain.append(" | ");
                }
                chain.append(cause.getMessage());
            }
            cause = cause.getCause();
        }
        assertThat(chain.toString(), containsString(fragment));
    }
}
