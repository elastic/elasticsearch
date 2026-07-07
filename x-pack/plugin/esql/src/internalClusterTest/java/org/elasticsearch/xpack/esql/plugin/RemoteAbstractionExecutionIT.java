/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.PlanTimeProfile;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.AbstractExternalDataSourceIT;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.RemoteDatasetExec;
import org.elasticsearch.xpack.esql.plan.physical.RemoteViewExec;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.Result;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.junit.Before;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * The Phase-7 Increment-3 gate — the end-to-end payoff. Proves a {@code Boundary.REMOTE} abstraction leaf actually
 * <b>EXECUTES</b> through the operator: a physical plan carrying a {@link RemoteViewExec}/{@link RemoteDatasetExec} is
 * driven through the real coordinator path ({@code ComputeService.execute} → {@code LocalExecutionPlanner}'s
 * {@code planRemoteAbstraction} branch → {@link RemoteAbstractionSourceOperator} → {@link FederationExecutionService}), and
 * the rows the operator drains equal a direct {@code FROM <name>} run — for both a view-over-index and an external-CSV
 * dataset home.
 *
 * <p><b>Forcing REMOTE.</b> Production never flips {@code Boundary.REMOTE} yet (that is Increment-4), so the test injects
 * the plan directly rather than reaching the leaf from a user query. The plan's leaf carries the abstraction name, the
 * {@link RemoteClusterAware#LOCAL_CLUSTER_GROUP_KEY empty} handle (the home cluster is this same single node, reached via
 * the local-node connection), and the schema resolved from a direct {@code FROM <name>} run — exactly the shape the
 * Increment-4 boundary flip will construct.
 *
 * <p><b>Multi-leaf collision.</b> {@link #testTwoForcedRemoteLeavesUnderSameSessionStayDisjoint} drives two forced-REMOTE
 * leaves whose child sessions share one base session id — the exact shape that trips the Increment-2 child-session
 * collision if the {@code <sessionId>/abstraction/<n>} namespacing regresses. It asserts each leaf drains its OWN rows,
 * proving the two exchanges stay disjoint (no cross-sink drain, no lost rows).
 *
 * <p>Single-node by design (no cross-cluster infra); lives in the {@code plugin} package to reach the package-private
 * {@link ComputeService} via {@code TransportEsqlQueryAction.getComputeService()} and the operator classes.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class RemoteAbstractionExecutionIT extends AbstractExternalDataSourceIT {

    private Path csvFixture;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    /** Determinism over planner-regression diversity — the injected plan pins a specific coordinator shape. */
    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    @Before
    public void writeFixtureAndRegister() throws Exception {
        csvFixture = createTempFile("remote-abstraction-exec-", ".csv");
        Files.writeString(csvFixture, String.join("\n", "emp_no:integer,first_name:keyword", "1,Alice", "2,Bob", "3,Carol") + "\n");
        registerDataSource("local_ds", Map.of());
        registerDataset("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"));
    }

    /**
     * The coordinator-only shape: a {@link RemoteDatasetExec} over an external CSV dataset. Injecting the plan and running
     * it through the operator must drain rows equal to a direct {@code FROM employees} run.
     */
    public void testForcedRemoteDatasetExecutesAndDrainsRows() throws Exception {
        Oracle oracle = oracleFor("FROM employees");
        PhysicalPlan plan = remoteDatasetPlan("employees", oracle.schema);

        List<List<Object>> drained = executeCoordinatorPlan(plan);
        assertThat(sortByColumn(drained, oracle.schema, "emp_no"), equalTo(sortByColumn(oracle.rows, oracle.schema, "emp_no")));
    }

    /**
     * The data-node-split shape: a {@link RemoteViewExec} over a VIEW over a real index. The view's body carries a
     * coordinator/data-node scan split; the operator must still drain rows equal to a direct {@code FROM emp_view} run.
     */
    public void testForcedRemoteViewOverRealIndexExecutesAndDrainsRows() throws Exception {
        createIndex("emp_index");
        prepareIndex("emp_index").setId("1").setSource(Map.of("emp_no", 1, "first_name", "Alice")).get();
        prepareIndex("emp_index").setId("2").setSource(Map.of("emp_no", 2, "first_name", "Bob")).get();
        prepareIndex("emp_index").setId("3").setSource(Map.of("emp_no", 3, "first_name", "Carol")).get();
        refresh("emp_index");
        assertAcked(
            client().execute(PutViewAction.INSTANCE, new PutViewAction.Request(TIMEOUT, TIMEOUT, new View("emp_view", "FROM emp_index")))
        );

        Oracle oracle = oracleFor("FROM emp_view");
        PhysicalPlan plan = remoteViewPlan("emp_view", oracle.schema);

        List<List<Object>> drained = executeCoordinatorPlan(plan);
        assertThat(sortByColumn(drained, oracle.schema, "emp_no"), equalTo(sortByColumn(oracle.rows, oracle.schema, "emp_no")));
    }

    /**
     * Two forced-REMOTE leaves whose child sessions share one base session id — the Increment-2 collision shape. Each
     * leaf's operator mints {@code <base>/abstraction/<n>} from the federation counter while {@code ComputeService} mints
     * {@code <base>/<n>} for the plans' own fragments from a separate counter. If the {@code /abstraction/} namespacing
     * regressed, the two counters would collide in the one exchange-id space and a leaf would competitively drain the
     * other's pages. Here we run two distinct datasets as two forced-REMOTE plans concurrently under the SAME base session
     * and assert each drains its OWN rows — disjoint, no cross-sink drain, no lost rows.
     */
    public void testTwoForcedRemoteLeavesUnderSameSessionStayDisjoint() throws Exception {
        // A second dataset with different data, so a cross-sink drain would produce visibly wrong rows.
        Path otherFixture = createTempFile("remote-abstraction-exec-other-", ".csv");
        Files.writeString(otherFixture, String.join("\n", "emp_no:integer,first_name:keyword", "10,Dan", "20,Eve") + "\n");
        registerDataset("contractors", "local_ds", otherFixture.toUri().toString(), Map.of("format", "csv"));

        Oracle employees = oracleFor("FROM employees");
        Oracle contractors = oracleFor("FROM contractors");

        // One shared base session id for both leaves — the collision-prone namespace.
        String baseSession = "remote-abstraction-multileaf-" + randomAlphaOfLength(8);
        PhysicalPlan planA = remoteDatasetPlan("employees", employees.schema);
        PhysicalPlan planB = remoteDatasetPlan("contractors", contractors.schema);

        // Drive both concurrently under the SAME base session so both leaves' operators draw from the one node-global
        // FederationExecutionService child-session counter while ComputeService mints its own <base>/<n> fragment
        // sessions off a separate counter — the exact two-counters-one-id-space shape the /abstraction/ namespace guards.
        PlainActionFuture<List<List<Object>>> futureA = new PlainActionFuture<>();
        PlainActionFuture<List<List<Object>>> futureB = new PlainActionFuture<>();
        submitCoordinatorPlan(baseSession, planA, futureA);
        submitCoordinatorPlan(baseSession, planB, futureB);

        List<List<Object>> drainedA = futureA.actionGet(TIMEOUT);
        List<List<Object>> drainedB = futureB.actionGet(TIMEOUT);

        assertThat(sortByColumn(drainedA, employees.schema, "emp_no"), equalTo(sortByColumn(employees.rows, employees.schema, "emp_no")));
        assertThat(
            sortByColumn(drainedB, contractors.schema, "emp_no"),
            equalTo(sortByColumn(contractors.rows, contractors.schema, "emp_no"))
        );
    }

    // ---- plan construction (the forced Boundary.REMOTE leaves) ----

    private static PhysicalPlan remoteDatasetPlan(String name, List<Attribute> output) {
        return new RemoteDatasetExec(Source.EMPTY, name, RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, output);
    }

    private static PhysicalPlan remoteViewPlan(String name, List<Attribute> output) {
        return new RemoteViewExec(Source.EMPTY, name, RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, output);
    }

    // ---- driving the real coordinator path ----

    /**
     * Drives {@code leafPlan} through {@link ComputeService#execute} on the local node — the real coordinator path that
     * plans it through {@code LocalExecutionPlanner.planRemoteAbstraction} into {@link RemoteAbstractionSourceOperator}
     * and collects the pages it drains — then returns the collected rows in row-major form.
     */
    private List<List<Object>> executeCoordinatorPlan(PhysicalPlan leafPlan) throws Exception {
        String sessionId = "remote-abstraction-exec-" + randomAlphaOfLength(8);
        PlainActionFuture<List<List<Object>>> future = new PlainActionFuture<>();
        submitCoordinatorPlan(sessionId, leafPlan, future);
        return future.actionGet(TIMEOUT);
    }

    private void submitCoordinatorPlan(String sessionId, PhysicalPlan leafPlan, PlainActionFuture<List<List<Object>>> resultRows) {
        String node = internalCluster().getNodeNames()[0];
        TransportService transportService = internalCluster().getInstance(TransportService.class, node);
        ThreadPool threadPool = transportService.getThreadPool();
        TaskManager taskManager = transportService.getTaskManager();
        ComputeService computeService = internalCluster().getInstance(TransportEsqlQueryAction.class, node).getComputeService();

        Configuration configuration = EsqlTestUtils.configuration("federation-exec-plan");
        EsqlQueryRequest rootRequest = EsqlQueryRequest.syncEsqlQueryRequest("federation-exec");
        CancellableTask rootTask = (CancellableTask) taskManager.register("transport", EsqlQueryAction.NAME, rootRequest);
        EsqlExecutionInfo execInfo = new EsqlExecutionInfo(clusterAlias -> false, EsqlExecutionInfo.IncludeExecutionMetadata.NEVER);

        // execute() asserts it runs on a search-family pool (it is normally invoked downstream of the analyzer callback).
        threadPool.executor(ThreadPool.Names.SEARCH).execute(() -> {
            try {
                computeService.execute(
                    sessionId,
                    rootTask,
                    computeService.createFlags(),
                    leafPlan,
                    configuration,
                    FoldContext.small(),
                    execInfo,
                    new PlanTimeProfile(),
                    new PlainActionFuture<Result>() {
                        @Override
                        public void onResponse(Result result) {
                            try {
                                resultRows.onResponse(pagesToRows(result.pages()));
                            } finally {
                                Releasables.close(() -> result.pages().forEach(p -> p.releaseBlocks()));
                                taskManager.unregister(rootTask);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            taskManager.unregister(rootTask);
                            resultRows.onFailure(e);
                        }
                    }
                );
            } catch (Exception e) {
                taskManager.unregister(rootTask);
                resultRows.onFailure(e);
            }
        });
    }

    // ---- oracle + row helpers ----

    private record Oracle(List<Attribute> schema, List<List<Object>> rows) {}

    private Oracle oracleFor(String query) {
        try (var response = run(EsqlQueryRequest.syncEsqlQueryRequest(query), TIMEOUT)) {
            return new Oracle(schemaOf(response), rowsOf(response));
        }
    }

    private static List<List<Object>> pagesToRows(List<Page> pages) {
        List<List<Object>> rows = new ArrayList<>();
        for (Page page : pages) {
            for (int p = 0; p < page.getPositionCount(); p++) {
                List<Object> row = new ArrayList<>(page.getBlockCount());
                for (int b = 0; b < page.getBlockCount(); b++) {
                    Block block = page.getBlock(b);
                    row.add(normalize(BlockUtils.toJavaObject(block, p)));
                }
                rows.add(row);
            }
        }
        return rows;
    }

    /** {@code BlockUtils.toJavaObject} returns keyword values as raw {@link BytesRef}; the response API returns Strings. */
    private static Object normalize(Object v) {
        return v instanceof BytesRef bytesRef ? bytesRef.utf8ToString() : v;
    }

    private static List<Attribute> schemaOf(EsqlQueryResponse response) {
        List<Attribute> schema = new ArrayList<>();
        for (ColumnInfoImpl c : response.columns()) {
            schema.add(new ReferenceAttribute(Source.EMPTY, c.name(), c.type()));
        }
        return schema;
    }

    private static List<List<Object>> rowsOf(EsqlQueryResponse response) {
        List<List<Object>> rows = new ArrayList<>();
        response.values().forEachRemaining(it -> {
            List<Object> row = new ArrayList<>();
            it.forEachRemaining(row::add);
            rows.add(row);
        });
        return rows;
    }

    /** Row order over an exchange is not guaranteed; sort by the named column for a stable comparison. */
    private static List<List<Object>> sortByColumn(List<List<Object>> rows, List<Attribute> schema, String column) {
        int idx = -1;
        for (int i = 0; i < schema.size(); i++) {
            if (schema.get(i).name().equals(column)) {
                idx = i;
                break;
            }
        }
        final int col = idx;
        List<List<Object>> sorted = new ArrayList<>(rows);
        sorted.sort((a, b) -> Integer.compare(((Number) a.get(col)).intValue(), ((Number) b.get(col)).intValue()));
        return sorted;
    }
}
