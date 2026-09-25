/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.Build;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.compute.data.LongVectorBlock;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperatorStatus;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.MockSearchService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.spatial.SpatialPlugin;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.singleValue;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Verifies that the {@link org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator}} is optimized into the reduce driver instead
 * of the data driver.
 *
 * For more information on why this is important, see {@link org.elasticsearch.xpack.esql.plugin.LateMaterializationPlanner}.
 */
public abstract class EsqlReductionLateMaterializationTestCase extends AbstractEsqlIntegTestCase {
    private final int shardCount;
    private final int maxConcurrentNodes;
    private final int taskConcurrency;

    EsqlReductionLateMaterializationTestCase(@Name("TestCase") TestCase testCase) {
        this.shardCount = testCase.shardCount;
        this.maxConcurrentNodes = testCase.maxConcurrentNodes;
        this.taskConcurrency = testCase.taskConcurrency;
    }

    @BeforeClass
    public static void checkCapabilities() {
        assumeTrue("pragmas only enabled on snapshot builds", Build.current().isSnapshot());
        assumeTrue("Node reduction must be enabled", EsqlCapabilities.Cap.ENABLE_REDUCE_NODE_LATE_MATERIALIZATION.isEnabled());
    }

    public record TestCase(int shardCount, int maxConcurrentNodes, int taskConcurrency) {}

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        var result = new ArrayList<Object[]>();
        for (int shardCount : new int[] { 1, 5 }) {
            for (int maxConcurrentNodes : new int[] { 1, 5 }) {
                for (int taskConcurrency : new int[] { 1, 5 }) {
                    result.add(new Object[] { new TestCase(shardCount, maxConcurrentNodes, taskConcurrency) });
                }
            }
        }
        return result;
    }

    private void setupIndex() throws Exception {
        XContentBuilder mapping = JsonXContent.contentBuilder().startObject();
        mapping.startObject("properties");
        {
            mapping.startObject("sorted").field("type", "long").endObject();
            mapping.startObject("filtered").field("type", "long").endObject();
            mapping.startObject("read").field("type", "long").endObject();
            mapping.startObject("more").field("type", "long").endObject();
            mapping.startObject("some_more").field("type", "long").endObject();
        }
        mapping.endObject();
        client().admin().indices().prepareCreate("test").setSettings(indexSettings(10, 0)).setMapping(mapping.endObject()).get();

        var builders = IntStream.range(0, 1024)
            .mapToObj(
                i -> prepareIndex("test").setId(Integer.toString(i))
                    .setSource("read", i, "sorted", i * 2, "filtered", i * 3, "more", i * 4, "some_more", i * 5)
            )
            .toList();
        indexRandom(true, builders);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopyNoNullElements(super.nodePlugins(), MockSearchService.TestPlugin.class, SpatialPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(PlannerSettings.REDUCTION_LATE_MATERIALIZATION.getKey(), true)
            .build();
    }

    public void testNoPushdowns() throws Exception {
        testLateMaterializationAfterReduceTopN(
            "from test | sort sorted + 1 desc | limit 3 | stats sum(read)",
            Set.of("sorted"),
            Set.of("read")
        );
    }

    public void testPushdownTopN() throws Exception {
        testLateMaterializationAfterReduceTopN(
            "from test | sort sorted desc | limit 3 | stats sum(read)",
            Set.of("sorted"),
            Set.of("read")
        );
    }

    public void testPushdownTopNMultipleSortedFields() throws Exception {
        testLateMaterializationAfterReduceTopN(
            "from test | sort sorted desc, more asc | limit 3 | stats sum(read)",
            Set.of("sorted", "more"),
            Set.of("read")
        );
    }

    public void testPushdownTopNMultipleRetrievedFields() throws Exception {
        testLateMaterializationAfterReduceTopN(
            "from test | sort sorted desc, more asc | limit 3 | stats x = sum(read), y = max(some_more)",
            Set.of("sorted", "more"),
            Set.of("read", "some_more")
        );
    }

    public void testPushdownTopFilterOnNonProjected() throws Exception {
        testLateMaterializationAfterReduceTopN(
            "from test | where filtered > 0 | sort sorted desc | limit 3 | stats sum(read)",
            Set.of("sorted"),
            Set.of("read")
        );
    }

    public void testPushdownTopFilterOnProjected() throws Exception {
        testLateMaterializationAfterReduceTopN(
            "from test | sort sorted desc | limit 3 | where filtered > 0 | stats sum(read)",
            Set.of("sorted"),
            Set.of("read", "filtered")
        );
    }

    /**
     * No narrowing {@code KEEP}, so the top-level {@code Project} that {@code ProjectAwayColumns} inserts covers the whole relation
     * and cannot say what has to cross the exchange. {@code filtered} is only read by the {@code WHERE}, so the data drivers must
     * not ship it: they only need to emit the sort key.
     */
    public void testNoKeepFilterOnProjected() throws Exception {
        setupIndex();
        try (var result = sendQuery("from test | where filtered > 0 | sort sorted desc | limit 3")) {
            assertThat(result.isRunning(), equalTo(false));
            assertThat(result.isPartial(), equalTo(false));
            assertSingleKeyFieldExtracted(result, "data", Set.of("sorted"));
            assertSingleKeyFieldExtracted(result, "node_reduce", Set.of("read", "filtered", "more", "some_more"));
            // The reduce driver re-reads these from _doc after its own TopN, so they have to line up with the sort key row for row.
            // A reload that returned the right values against the wrong rows would satisfy the profile assertions above.
            assertThat(column(result, "sorted"), equalTo(List.of(2046L, 2044L, 2042L)));
            assertThat(column(result, "read"), equalTo(List.of(1023L, 1022L, 1021L)));
            assertThat(column(result, "filtered"), equalTo(List.of(3069L, 3066L, 3063L)));
        }
    }

    /**
     * A {@code FORK} branch never has a narrowing {@code KEEP} - {@code ProjectAwayColumns} keeps everything {@code MergeExec} needs -
     * which is exactly the shape that used to defeat the pruning and made every branch load all fields in every data driver.
     *
     * <p>Both branches' drivers report the same unqualified {@code "data"} / {@code "node_reduce"} descriptions, so each assertion is
     * the aggregate over the branches. That is the right metric anyway: the cost this fixes is the per-slice fan-out summed over
     * branches. The {@code node_reduce} half matters as much as the {@code data} half - without it the test would also pass if the
     * branches stopped loading the deferred fields altogether rather than deferring them.
     */
    public void testForkBranchesLateMaterialize() throws Exception {
        assumeTrue("requires FORK", EsqlCapabilities.Cap.FORK_V9.isEnabled());
        setupIndex();
        String query = """
            from test
            | fork ( where filtered > 0 | sort sorted desc | limit 3 )
                   ( where more > 0     | sort sorted desc | limit 3 )
            | sort _fork, sorted desc
            """;
        try (var result = sendQuery(query)) {
            assertThat(result.isRunning(), equalTo(false));
            assertThat(result.isPartial(), equalTo(false));
            assertSingleKeyFieldExtracted(result, "data", Set.of("sorted"));
            assertSingleKeyFieldExtracted(result, "node_reduce", Set.of("read", "filtered", "more", "some_more"));
            // Both branches keep the same three documents - only i=0 fails either predicate - and _fork orders the branches.
            assertThat(column(result, "sorted"), equalTo(List.of(2046L, 2044L, 2042L, 2046L, 2044L, 2042L)));
            assertThat(column(result, "read"), equalTo(List.of(1023L, 1022L, 1021L, 1023L, 1022L, 1021L)));
        }
    }

    /**
     * The values of column {@code name}, in row order. Used to check that the columns the node-reduce driver reloads from
     * {@code _doc} line up with the rows its pipeline breaker emitted.
     */
    private static List<Object> column(EsqlQueryResponse response, String name) {
        List<String> names = response.columns().stream().map(ColumnInfo::name).toList();
        int index = names.indexOf(name);
        assertThat("no column [" + name + "] in " + names, index, greaterThanOrEqualTo(0));
        return getValuesList(response).stream().map(row -> row.get(index)).toList();
    }

    private void testLateMaterializationAfterReduceTopN(
        String query,
        Set<String> expectedDataLoadedFields,
        Set<String> expectedNodeReduceFields
    ) throws Exception {
        setupIndex();
        try (var result = sendQuery(query)) {
            assertThat(result.isRunning(), equalTo(false));
            assertThat(result.isPartial(), equalTo(false));
            assertSingleKeyFieldExtracted(result, "data", expectedDataLoadedFields);
            assertSingleKeyFieldExtracted(result, "node_reduce", expectedNodeReduceFields);
            var page = singleValue(result.pages());
            assertThat(page.getPositionCount(), equalTo(1));
            LongVectorBlock block = page.getBlock(0);
            assertThat(block.getPositionCount(), equalTo(1));
            assertThat(block.getLong(0), equalTo(1021L + 1022 + 1023));
        }
    }

    private static void assertSingleKeyFieldExtracted(EsqlQueryResponse response, String driverName, Set<String> expectedLoadedFields) {
        long totalValuesLoader = 0;
        for (var driverProfile : response.profile().drivers().stream().filter(d -> d.description().equals(driverName)).toList()) {
            OperatorStatus operatorStatus = singleValue(
                Strings.format(
                    "Only a single ValuesSourceReaderOperator should be present in driver '%s'; "
                        + "more than that means we didn't move the operator in the planner correctly",
                    driverName
                ),
                driverProfile.operators().stream().filter(o -> o.operator().startsWith("ValuesSourceReaderOperator")).toList()
            );
            var status = (ValuesSourceReaderOperatorStatus) operatorStatus.status();
            totalValuesLoader += status.valuesLoaded();
            if (status.valuesLoaded() == 0) {
                // This can happen if the indexRandom created dummy documents which led to empty segments.
                continue;
            }
            assertThat(status.readersBuilt().size(), equalTo(expectedLoadedFields.size()));
            for (String field : status.readersBuilt().keySet()) {
                assertTrue(
                    "Field " + field + " was not expected to be loaded in driver " + driverName,
                    expectedLoadedFields.stream().anyMatch(field::contains)
                );
            }
        }
        assertThat("Values should have been loaded", totalValuesLoader, greaterThan(0L));
    }

    private EsqlQueryResponse sendQuery(String query) {
        // Ensures there is no TopN pushdown to lucene, and that the pause happens after the TopN operator has been applied.
        return client().execute(
            EsqlQueryAction.INSTANCE,
            syncEsqlQueryRequest(query).pragmas(
                new QueryPragmas(
                    Settings.builder()
                        // Configured to ensure that there is only one worker handling all the shards, so that we can assert the correct
                        // expected behavior.
                        .put(QueryPragmas.MAX_CONCURRENT_NODES_PER_CLUSTER.getKey(), maxConcurrentNodes)
                        .put(QueryPragmas.MAX_CONCURRENT_SHARDS_PER_NODE.getKey(), shardCount)
                        .put(QueryPragmas.TASK_CONCURRENCY.getKey(), taskConcurrency)
                        .put(QueryPragmas.NODE_LEVEL_REDUCTION.getKey(), true)
                        .build()
                )
            ).profile(true)
        ).actionGet(1, TimeUnit.MINUTES);
    }
}
