/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.tablefunction;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.action.cat.RestShardsAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelationBuilder;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

/**
 * The parts of {@code CAT} that do not need a cluster: what it refuses, what columns it declares, and what it
 * makes of a {@link Table}'s cells. The integration test covers the two requests and the query; everything here
 * is reachable from a table built in the test, which is why the conversion takes one.
 */
public class CatTableFunctionTests extends ESTestCase {

    private static final CatTableFunction FUNCTION = new CatTableFunction(null);

    private ThreadPool threadPool;
    private CircuitBreaker breaker;
    private BlockFactory blockFactory;

    @Before
    public void startThreadPoolAndBreaker() {
        threadPool = new TestThreadPool(getTestName());
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofKb(256)).withCircuitBreaking();
        breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        blockFactory = new BlockFactory(breaker, bigArrays);
    }

    @After
    public void stopThreadPool() {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testAnUnknownTableIsRefusedAtItsOwnArgument() {
        ParsingException e = expectThrows(ParsingException.class, () -> FUNCTION.validate(call("nosuchtable", "logs-*")));
        assertThat(e.getMessage(), containsString("[cat] has no table named [nosuchtable]"));
    }

    /** The table is a name a user types, and {@code TableFunctionRegistry} resolves those without regard to case. */
    public void testTheTableNameIsCaseInsensitive() {
        for (String table : List.of("shards", "SHARDS", "Shards")) {
            FUNCTION.validate(call(table, "logs-*"));
        }
    }

    /**
     * A pattern that splits to nothing. {@code ","} is not blank, and an empty indices array on the two requests
     * means every index in the cluster — so the guard reads the split rather than the string.
     */
    public void testAPatternThatNamesNoIndexIsRefused() {
        for (String pattern : List.of("", "   ", ",", ",,")) {
            ParsingException e = expectThrows(ParsingException.class, () -> FUNCTION.validate(call("shards", pattern)));
            assertThat(e.getMessage(), containsString("requires an index pattern"));
        }
    }

    public void testTheSignatureTakesATableAndAPattern() {
        assertThat(FUNCTION.signature().arguments().size(), equalTo(2));
    }

    /** The columns are the cat action's own, and every one is a keyword because a cat cell is text. */
    public void testTheColumnsComeFromTheCatActionAndAreAllKeywords() {
        Table header = RestShardsAction.getTableWithHeader(RestApiVersion.current());
        LocalRelation relation = written(header);
        List<Attribute> columns = relation.output();

        List<String> expected = header.getHeaders().stream().map(cell -> String.valueOf(cell.value)).toList();
        assertThat(columns.stream().map(Attribute::name).toList(), equalTo(expected));
        assertThat(columns.stream().map(Attribute::dataType).toList(), everyItem(equalTo(DataType.KEYWORD)));
        relation.supplier().get().releaseBlocks();
    }

    /**
     * A cell renders the way it renders itself, which for a size is text and not a number — the reason the
     * columns are keywords and {@code SUM(store)} does not work without a cast.
     */
    public void testACellRendersAsItsOwnText() {
        Table table = table(List.of("0", ByteSizeValue.ofBytes(227), 42));

        assertThat(row(table, 0), contains("0", "227b", "42"));
    }

    /** A null cell is an absent value. Rendering it would put the four letters in the column. */
    public void testANullCellStaysNull() {
        Table table = table(List.of("0", "227b", "x"));
        table.startRow();
        table.addCell("1");
        table.addCell(null);
        table.addCell(null);
        table.endRow();

        assertThat(row(table, 1), contains(equalTo("1"), nullValue(), nullValue()));
    }

    /**
     * Past the cap the read is refused naming the pattern, as an {@code IllegalArgumentException} so it reaches
     * the caller as a 400: too many shards is their input and their fix. The base class's own cap would throw an
     * {@code IllegalStateException}, which maps to a 500.
     */
    public void testPastTheCapTheReadIsRefusedAsABadRequest() {
        Table table = oneColumnTable(CatTableFunction.MAX_ROWS + 1);

        try (LocalRelationBuilder rows = new LocalRelationBuilder(CatTableFunction.NAME, TestBlockFactory.getNonBreakingInstance())) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> CatTableFunction.write("logs-*", table, rows));
            assertThat(e.getMessage(), containsString("matched [10001] shards for [logs-*]"));
            assertThat(e.getMessage(), containsString("narrow the index pattern"));
        }
    }

    /** And at the cap exactly, which is the row count a {@code >=} would wrongly refuse. */
    public void testAtTheCapTheReadStillHappens() {
        Table table = oneColumnTable(CatTableFunction.MAX_ROWS);

        try (Page page = written(table).supplier().get()) {
            assertThat(page.getPositionCount(), equalTo(CatTableFunction.MAX_ROWS));
        }
    }

    /**
     * Both arguments are names. A {@code {...}} where the table belongs is refused by the shared signature check,
     * which runs before {@code validate} — so the function's own refusals never see one.
     */
    public void testBothArgumentsMustBeNames() {
        TableFunctionInvocation withAMap = new TableFunctionInvocation(
            Source.EMPTY,
            CatTableFunction.NAME,
            List.of(new TableFunctionArgument.NamedValues(Source.EMPTY, Map.of()), new TableFunctionArgument.Name(Source.EMPTY, "logs-*"))
        );

        ParsingException e = expectThrows(ParsingException.class, () -> FUNCTION.signature().check(withAMap));
        assertThat(e.getMessage(), containsString("[cat] expects a name for [table]"));
    }

    /**
     * The cluster-state half failing fails the read rather than escaping, and the request it failed on is the one
     * {@code _cat/shards} would have made: the pattern split on commas, with closed and hidden indices matched.
     * A query cannot make either request fail, so this is the only place the two shapes are read back.
     */
    public void testAFailedClusterStateRequestFailsTheRead() {
        ScriptedClient client = new ScriptedClient(threadPool);
        client.stateAnswer = request -> { throw new IllegalStateException("no master"); };

        PlainActionFuture<?> future = read(client, "logs-*,metrics-*");

        IllegalStateException e = expectThrows(IllegalStateException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("no master"));
        assertThat(client.stateRequests, hasSize(1));
        assertThat(client.stateRequests.get(0).indices(), equalTo(new String[] { "logs-*", "metrics-*" }));
        assertThat(client.stateRequests.get(0).indicesOptions(), equalTo(IndicesOptions.strictExpandHidden()));
        assertThat("the stats half waits on the first", client.statsRequests, hasSize(0));
    }

    /** And the stats half, which is dispatched only once the first answered. */
    public void testAFailedStatsRequestFailsTheRead() {
        ScriptedClient client = new ScriptedClient(threadPool);
        client.stateAnswer = request -> emptyClusterState();
        client.statsAnswer = request -> { throw new IllegalStateException("shard gone"); };

        PlainActionFuture<?> future = read(client, "logs-*");

        IllegalStateException e = expectThrows(IllegalStateException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("shard gone"));
        assertThat(client.statsRequests, hasSize(1));
        IndicesStatsRequest stats = client.statsRequests.get(0);
        assertThat(stats.indices(), equalTo(new String[] { "logs-*" }));
        assertThat(stats.indicesOptions(), equalTo(IndicesOptions.strictExpandHidden()));
    }

    /** The relation CAT writes for {@code table}, whose blocks the caller owns and releases. */
    private static LocalRelation written(Table table) {
        try (LocalRelationBuilder rows = new LocalRelationBuilder(CatTableFunction.NAME, TestBlockFactory.getNonBreakingInstance())) {
            CatTableFunction.write("logs-*", table, rows);
            return rows.relation(Source.EMPTY);
        }
    }

    /** One row of what CAT wrote, as the strings a reader sees, with an absent cell as null. */
    private static List<String> row(Table table, int position) {
        try (Page page = written(table).supplier().get()) {
            List<String> cells = new ArrayList<>(page.getBlockCount());
            BytesRef scratch = new BytesRef();
            for (int c = 0; c < page.getBlockCount(); c++) {
                BytesRefBlock block = page.getBlock(c);
                cells.add(block.isNull(position) ? null : block.getBytesRef(position, scratch).utf8ToString());
            }
            return cells;
        }
    }

    private static TableFunctionInvocation call(String table, String pattern) {
        return new TableFunctionInvocation(
            Source.EMPTY,
            CatTableFunction.NAME,
            List.of(new TableFunctionArgument.Name(Source.EMPTY, table), new TableFunctionArgument.Name(Source.EMPTY, pattern))
        );
    }

    /** {@code rows} rows of one column, which is what the cap counts. */
    private static Table oneColumnTable(int rows) {
        Table table = new Table();
        table.startHeaders().addCell("index").endHeaders();
        for (int i = 0; i < rows; i++) {
            table.startRow();
            table.addCell("i" + i);
            table.endRow();
        }
        return table;
    }

    /** {@code toRelation} over {@code client}, on a tracked factory so a block it fails to release reddens teardown. */
    private PlainActionFuture<LeafPlan> read(ScriptedClient client, String pattern) {
        CatTableFunction function = new CatTableFunction(client) {
            @Override
            BlockFactory blockFactory() {
                return blockFactory;
            }
        };
        PlainActionFuture<LeafPlan> future = new PlainActionFuture<>();
        function.toRelation(call("shards", pattern), List.of(), null, future);
        return future;
    }

    private static ClusterStateResponse emptyClusterState() {
        ClusterName name = new ClusterName("test");
        return new ClusterStateResponse(name, ClusterState.builder(name).build(), false);
    }

    /** Answers the two requests {@code CAT} makes, or throws for one, and records both. */
    private static final class ScriptedClient extends NoOpNodeClient {
        final List<ClusterStateRequest> stateRequests = new ArrayList<>();
        final List<IndicesStatsRequest> statsRequests = new ArrayList<>();
        Function<ClusterStateRequest, ClusterStateResponse> stateAnswer = request -> {
            throw new AssertionError("unexpected cluster state request");
        };
        Function<IndicesStatsRequest, ActionResponse> statsAnswer = request -> { throw new AssertionError("unexpected stats request"); };

        ScriptedClient(ThreadPool threadPool) {
            super(threadPool);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            try {
                if (action == ClusterStateAction.INSTANCE) {
                    stateRequests.add((ClusterStateRequest) request);
                    listener.onResponse((Response) stateAnswer.apply((ClusterStateRequest) request));
                    return;
                }
                assertThat(action, equalTo(IndicesStatsAction.INSTANCE));
                statsRequests.add((IndicesStatsRequest) request);
                listener.onResponse((Response) statsAnswer.apply((IndicesStatsRequest) request));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }
    }

    /** One header per value, one row holding them, which is all the conversion looks at. */
    private static Table table(List<Object> values) {
        Table table = new Table();
        table.startHeaders();
        for (int i = 0; i < values.size(); i++) {
            table.addCell("c" + i);
        }
        table.endHeaders();
        table.startRow();
        for (Object value : values) {
            table.addCell(value);
        }
        table.endRow();
        return table;
    }
}
