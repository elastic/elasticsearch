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
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.exchange.ExchangeSource;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
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
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.session.Configuration;
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
 * The Phase-7 Increment-2 gate. Proves the coordinator-side dispatcher — {@link FederationExecutionService} — packages the
 * openExchange → sendChildRequest({@link ExecuteAbstractionRequest}) → newRemoteSink → addRemoteSink dance
 * ({@code ClusterComputeHandler.startComputeOnRemoteCluster} for a single leaf) into one reusable
 * {@link FederationExecutionService#fetchAbstraction} call, and that draining the caller-owned leaf source yields rows
 * equal to a direct {@code FROM <name>} run.
 *
 * <p>Where Increment-1's {@code ExecuteAbstractionIT} drove the dispatch dance <em>inline</em> in the test to prove the
 * home-cluster handler, this drives it through the SERVICE — the exact node singleton
 * {@code TransportEsqlQueryAction} builds and threads into {@code ComputeService}. It covers BOTH the coordinator-only
 * external-CSV dataset and the data-node-split view-over-index shape, exactly as Increment-1 did, so the service is proven
 * on both {@code ExchangeSinkExec}-rooting regimes. The {@code LocalExecutionPlanner} source-operator wiring is
 * Increment-3 and deliberately not exercised here.
 *
 * <p>Single-node by design: the home cluster is the local node, reached via the empty
 * ({@link RemoteClusterAware#LOCAL_CLUSTER_GROUP_KEY}) handle — the same local-connection path the service resolves for a
 * same-cluster abstraction. The service is package-private with a package-private constructor, so this test constructs the
 * very same instance shape and asserts the dispatch it performs, rather than reaching into the wired-up node singleton.
 *
 * <p>Lives in the {@code plugin} package so it can construct {@link FederationExecutionService} and the package-private
 * {@link ExecuteAbstractionRequest} carrier; extends the {@code public} {@link AbstractExternalDataSourceIT} for the
 * dataset CRUD helpers.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class FederationExecutionServiceIT extends AbstractExternalDataSourceIT {

    private Path csvFixture;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    /** Determinism over planner-regression diversity — the exchange dispatch pins a specific plan shape. */
    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    @Before
    public void writeFixtureAndRegister() throws Exception {
        csvFixture = createTempFile("federation-exec-fixture-", ".csv");
        Files.writeString(csvFixture, String.join("\n", "emp_no:integer,first_name:keyword", "1,Alice", "2,Bob", "3,Carol") + "\n");
        registerDataSource("local_ds", Map.of());
        registerDataset("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"));
    }

    /**
     * The proof: hand {@link FederationExecutionService#fetchAbstraction} an external-CSV dataset name and its resolved
     * schema, drain the leaf source it wires, and assert the rows equal a direct {@code FROM employees} run — the
     * coordinator-only shape (no data-node scan), dispatched entirely through the service.
     */
    public void testFetchAbstractionSinksRowsMatchingDirectRun() throws Exception {
        List<List<Object>> expectedRows;
        List<Attribute> expectedSchema;
        try (var response = run(EsqlQueryRequest.syncEsqlQueryRequest("FROM employees | SORT emp_no"), TIMEOUT)) {
            expectedSchema = schemaOf(response);
            expectedRows = rowsOf(response);
        }

        List<List<Object>> drained = fetchAndDrain("employees", expectedSchema);
        assertThat(sortByColumn(drained, expectedSchema, "emp_no"), equalTo(sortByColumn(expectedRows, expectedSchema, "emp_no")));
    }

    /**
     * The data-node-split shape: a VIEW over a real index, whose body carries a coordinator/data-node split. Dispatching
     * the view name through the service and draining must still match a direct {@code FROM view} run — proving the service
     * wires the sink correctly for the distributed plan shape too, not just the coordinator-only external case.
     */
    public void testFetchViewOverRealIndexSinksRows() throws Exception {
        createIndex("emp_index");
        prepareIndex("emp_index").setId("1").setSource(Map.of("emp_no", 1, "first_name", "Alice")).get();
        prepareIndex("emp_index").setId("2").setSource(Map.of("emp_no", 2, "first_name", "Bob")).get();
        prepareIndex("emp_index").setId("3").setSource(Map.of("emp_no", 3, "first_name", "Carol")).get();
        refresh("emp_index");
        assertAcked(
            client().execute(PutViewAction.INSTANCE, new PutViewAction.Request(TIMEOUT, TIMEOUT, new View("emp_view", "FROM emp_index")))
        );

        List<List<Object>> expectedRows;
        List<Attribute> expectedSchema;
        try (var response = run(EsqlQueryRequest.syncEsqlQueryRequest("FROM emp_view"), TIMEOUT)) {
            expectedSchema = schemaOf(response);
            expectedRows = rowsOf(response);
        }

        List<List<Object>> drained = fetchAndDrain("emp_view", expectedSchema);
        assertThat(sortByColumn(drained, expectedSchema, "emp_no"), equalTo(sortByColumn(expectedRows, expectedSchema, "emp_no")));
    }

    /**
     * Failure propagation: a schema that disagrees with the home cluster's fresh resolution makes the home handler fail
     * loud (B1). The service must surface that as an exceptional completion of the caller's listener — the dispatch does
     * NOT silently return fewer rows. Drives the same drift the Increment-1 handler test asserts, but through the service,
     * to prove the service's failFast wiring finishes the leaf source with the failure rather than hanging.
     */
    public void testFetchWithSchemaDriftCompletesExceptionally() throws Exception {
        List<Attribute> wrongSchema = List.of(
            new ReferenceAttribute(Source.EMPTY, "first_name", org.elasticsearch.xpack.esql.core.type.DataType.INTEGER),
            new ReferenceAttribute(Source.EMPTY, "emp_no", org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD)
        );

        Exception e = expectThrows(Exception.class, () -> fetchAndDrain("employees", wrongSchema));
        assertThat(exceptionChainMessage(e), org.hamcrest.Matchers.containsString("schema drift executing abstraction [employees]"));
    }

    /**
     * Drives {@link FederationExecutionService#fetchAbstraction} for a single leaf against the local node (empty handle),
     * then drains the caller-owned source it wires into row-major values. This is the exact call
     * {@code RemoteAbstractionSourceOperator} will make once per leaf in Increment-3.
     */
    private List<List<Object>> fetchAndDrain(String abstractionName, List<Attribute> expectedSchema) throws Exception {
        String node = internalCluster().getNodeNames()[0];
        TransportService transportService = internalCluster().getInstance(TransportService.class, node);
        ExchangeService exchangeService = internalCluster().getInstance(ExchangeService.class, node);
        ThreadPool threadPool = transportService.getThreadPool();
        TaskManager taskManager = transportService.getTaskManager();

        FederationExecutionService service = new FederationExecutionService(
            transportService,
            exchangeService,
            threadPool.executor(ThreadPool.Names.SEARCH)
        );

        String sessionId = "federation-exec-it-" + randomAlphaOfLength(8);
        int bufferSize = QueryPragmas.EMPTY.exchangeBufferSize();
        Configuration configuration = EsqlTestUtils.configuration("FROM " + abstractionName);

        // A real, cancellable root task to parent the child request the service dispatches (the request mandates a
        // parent). An EsqlQueryRequest is a convenient TaskAwareRequest that registers as a CancellableTask.
        EsqlQueryRequest rootRequest = EsqlQueryRequest.syncEsqlQueryRequest("FROM " + abstractionName);
        CancellableTask rootTask = (CancellableTask) taskManager.register("transport", EsqlQueryAction.NAME, rootRequest);

        try {
            // The caller (Increment-3's operator factory) owns the leaf source; the service only adds a sink into it.
            ExchangeSourceHandler leafSource = new ExchangeSourceHandler(bufferSize, threadPool.executor(ThreadPool.Names.SEARCH));

            PlainActionFuture<Void> completion = new PlainActionFuture<>();
            service.fetchAbstraction(
                sessionId,
                rootTask,
                configuration,
                RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
                abstractionName,
                expectedSchema,
                leafSource,
                completion
            );

            List<List<Object>> rows = drain(leafSource.createExchangeSource());

            // The completion (the sink finished draining) must succeed for the dispatch to be correct; a home-side
            // failure finishes it exceptionally, which surfaces here.
            completion.actionGet(TIMEOUT);
            return rows;
        } finally {
            taskManager.unregister(rootTask);
        }
    }

    /** Blocking drain of an exchange source into row-major values. */
    private List<List<Object>> drain(ExchangeSource source) {
        List<List<Object>> rows = new ArrayList<>();
        while (source.isFinished() == false) {
            Page page = source.pollPage();
            if (page == null) {
                var blocked = source.waitForReading();
                if (blocked.listener().isDone() == false) {
                    PlainActionFuture<Void> f = new PlainActionFuture<>();
                    blocked.listener().addListener(f);
                    f.actionGet(TIMEOUT);
                }
                continue;
            }
            try {
                for (int p = 0; p < page.getPositionCount(); p++) {
                    List<Object> row = new ArrayList<>(page.getBlockCount());
                    for (int b = 0; b < page.getBlockCount(); b++) {
                        Block block = page.getBlock(b);
                        row.add(normalize(BlockUtils.toJavaObject(block, p)));
                    }
                    rows.add(row);
                }
            } finally {
                page.releaseBlocks();
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

    private static String exceptionChainMessage(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (Throwable cur = t; cur != null; cur = cur.getCause()) {
            if (cur.getMessage() != null) {
                sb.append(cur.getMessage()).append(" | ");
            }
        }
        return sb.toString();
    }
}
