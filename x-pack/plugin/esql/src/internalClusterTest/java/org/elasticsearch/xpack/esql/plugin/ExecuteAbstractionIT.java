/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListenerResponseHandler;
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
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportRequestOptions;
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
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.junit.Before;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * The Phase-7 Increment-1 de-risk gate. Proves the deepest uncertainty of ES|QL federation execution: a cluster can,
 * given <b>only an abstraction's NAME</b>, resolve it through its OWN {@code SchemaService.resolvePlan} umbrella, plan the
 * body, run the existing compute path, and sink result pages into a named exchange — via {@link AbstractionComputeHandler},
 * the home-cluster sibling of {@link ClusterComputeHandler}.
 *
 * <p>Single-node by design (no cross-cluster infra): it performs the coordinator-dispatch dance directly —
 * {@code openExchange} a session, send {@link ExecuteAbstractionRequest} (name + expected schema) to the handler, drain
 * the exchange via {@code newRemoteSink}, and assert the rows match a direct {@code FROM <name>} run. It also asserts the
 * schema-drift guard (B1) fires: a request whose expected schema disagrees with the home cluster's fresh resolution
 * fails loud rather than returning positionally-mis-bound columns. The operator / {@code LocalExecutionPlanner} wiring is
 * Increment-3 and deliberately NOT exercised here — this proves the remote handler in isolation.
 *
 * <p>Lives in the {@code plugin} package so it can construct the package-private {@link ExecuteAbstractionRequest} and
 * read the package-private {@link ComputeResponse}; it extends the {@code public} {@link AbstractExternalDataSourceIT}
 * for the dataset CRUD helpers.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class ExecuteAbstractionIT extends AbstractExternalDataSourceIT {

    private static final String EXECUTE_ABSTRACTION_ACTION_NAME = EsqlQueryAction.NAME + "/execute_abstraction";

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
        csvFixture = createTempFile("execabstraction-fixture-", ".csv");
        Files.writeString(csvFixture, String.join("\n", "emp_no:integer,first_name:keyword", "1,Alice", "2,Bob", "3,Carol") + "\n");
        registerDataSource("local_ds", Map.of());
        registerDataset("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"));
    }

    /**
     * The proof: dispatch {@code ExecuteAbstractionRequest("employees", <correct schema>)} to the handler, drain the
     * exchange, and assert the rows equal a direct {@code FROM employees} run — end-to-end name → resolve → plan → run →
     * sink → drain, all on the home cluster, driven only by the name.
     */
    public void testExecuteAbstractionByNameSinksRowsMatchingDirectRun() throws Exception {
        List<List<Object>> expectedRows;
        List<Attribute> expectedSchema;
        try (var response = run(EsqlQueryRequest.syncEsqlQueryRequest("FROM employees | SORT emp_no"), TIMEOUT)) {
            expectedSchema = schemaOf(response);
            expectedRows = rowsOf(response);
        }

        List<List<Object>> drained = dispatchAndDrain("employees", expectedSchema);
        assertThat(sortByColumn(drained, expectedSchema, "emp_no"), equalTo(sortByColumn(expectedRows, expectedSchema, "emp_no")));
    }

    /**
     * The data-node-split path: a VIEW over a real index. Unlike the external CSV dataset (coordinator-only), a view
     * over an index produces a plan with a data-node scan, so the sink attaches on top of a plan that already carries a
     * coordinator/data-node split. Dispatching the view name and draining must still match a direct {@code FROM view}
     * run — proving the {@code ExchangeSinkExec} wrapping is correct for the distributed shape too, not just the
     * coordinator-only external case.
     */
    public void testExecuteViewOverRealIndexSinksRows() throws Exception {
        createIndex("emp_index");
        prepareIndex("emp_index").setId("1").setSource(Map.of("emp_no", 1, "first_name", "Alice")).get();
        prepareIndex("emp_index").setId("2").setSource(Map.of("emp_no", 2, "first_name", "Bob")).get();
        prepareIndex("emp_index").setId("3").setSource(Map.of("emp_no", 3, "first_name", "Carol")).get();
        refresh("emp_index");
        assertAcked(
            client().execute(PutViewAction.INSTANCE, new PutViewAction.Request(TIMEOUT, TIMEOUT, new View("emp_view", "FROM emp_index")))
        );

        // The oracle must use the same bare shape the dispatch runs (FROM <name>), so the expected schema matches what
        // the home cluster freshly resolves — otherwise B1 (correctly) fires.
        List<List<Object>> expectedRows;
        List<Attribute> expectedSchema;
        try (var response = run(EsqlQueryRequest.syncEsqlQueryRequest("FROM emp_view"), TIMEOUT)) {
            expectedSchema = schemaOf(response);
            expectedRows = rowsOf(response);
        }

        List<List<Object>> drained = dispatchAndDrain("emp_view", expectedSchema);
        assertThat(sortByColumn(drained, expectedSchema, "emp_no"), equalTo(sortByColumn(expectedRows, expectedSchema, "emp_no")));
    }

    /**
     * The schema-drift guard (B1): if the request's expected schema disagrees with what the home cluster freshly
     * resolves, the handler fails loud — it does NOT sink positionally-mis-bound pages. Here we lie about the schema
     * (swap the column names/types) and assert the dispatch fails with a schema-drift error.
     */
    public void testSchemaDriftFailsLoud() throws Exception {
        List<Attribute> wrongSchema = List.of(
            new ReferenceAttribute(Source.EMPTY, "first_name", DataType.INTEGER),
            new ReferenceAttribute(Source.EMPTY, "emp_no", DataType.KEYWORD)
        );

        Exception e = expectThrows(Exception.class, () -> dispatchAndDrain("employees", wrongSchema));
        assertThat(exceptionChainMessage(e), containsString("schema drift executing abstraction [employees]"));
    }

    /**
     * Performs the coordinator-dispatch dance directly (mirroring {@code ClusterComputeHandler.startComputeOnRemoteCluster}
     * for a single leaf, but against the local node): open an exchange for a fresh session, send the
     * {@link ExecuteAbstractionRequest} to {@link AbstractionComputeHandler}, add a remote sink into a source handler, and
     * drain the pages into row-major {@code List<List<Object>>}.
     */
    private List<List<Object>> dispatchAndDrain(String abstractionName, List<Attribute> expectedSchema) throws Exception {
        String node = internalCluster().getNodeNames()[0];
        TransportService transportService = internalCluster().getInstance(TransportService.class, node);
        ExchangeService exchangeService = internalCluster().getInstance(ExchangeService.class, node);
        ThreadPool threadPool = transportService.getThreadPool();
        TaskManager taskManager = transportService.getTaskManager();

        String sessionId = "exec-abstraction-it-" + randomAlphaOfLength(8);
        Transport.Connection connection = transportService.getLocalNodeConnection();
        int bufferSize = QueryPragmas.EMPTY.exchangeBufferSize();

        // A real, cancellable root task to parent the child request (the request mandates a parent task). An
        // EsqlQueryRequest is a convenient TaskAwareRequest that registers as a CancellableTask.
        EsqlQueryRequest rootRequest = EsqlQueryRequest.syncEsqlQueryRequest("FROM " + abstractionName);
        CancellableTask rootTask = (CancellableTask) taskManager.register("transport", EsqlQueryAction.NAME, rootRequest);

        try {
            ExchangeSourceHandler sourceHandler = new ExchangeSourceHandler(bufferSize, threadPool.executor(ThreadPool.Names.SEARCH));

            PlainActionFuture<Void> openFuture = new PlainActionFuture<>();
            ExchangeService.openExchange(
                transportService,
                connection,
                sessionId,
                bufferSize,
                threadPool.executor(ThreadPool.Names.SEARCH),
                openFuture
            );
            openFuture.actionGet(TIMEOUT);

            // sendChildRequest sets the parent task from rootTask, satisfying the request's mandatory-parent invariant.
            ExecuteAbstractionRequest request = new ExecuteAbstractionRequest(
                "",
                sessionId,
                EsqlTestUtils.configuration("FROM " + abstractionName),
                abstractionName,
                expectedSchema
            );

            PlainActionFuture<Void> responseFuture = new PlainActionFuture<>();
            transportService.sendChildRequest(
                connection,
                EXECUTE_ABSTRACTION_ACTION_NAME,
                request,
                rootTask,
                TransportRequestOptions.EMPTY,
                new ActionListenerResponseHandler<>(
                    responseFuture.map(r -> null),
                    ComputeResponse::new,
                    threadPool.executor(ThreadPool.Names.SEARCH)
                )
            );

            PlainActionFuture<Void> sinkFuture = new PlainActionFuture<>();
            var remoteSink = exchangeService.newRemoteSink(rootTask, sessionId, transportService, connection);
            sourceHandler.addRemoteSink(remoteSink, true, () -> {}, 1, sinkFuture);

            List<List<Object>> rows = drain(sourceHandler.createExchangeSource());

            // Both the compute response and the sink completion must succeed for the dispatch to be correct.
            responseFuture.actionGet(TIMEOUT);
            sinkFuture.actionGet(TIMEOUT);
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
