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
import org.elasticsearch.action.index.IndexRequestBuilder;
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
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.spatial.SpatialPlugin;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.singleValue;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

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
     * No narrowing {@code KEEP}, so the top-level {@code Project} that {@code ProjectAwayColumns} inserts covers the whole relation.
     * The {@code WHERE} reads {@code filtered} and the query returns it. The filter is pushed to Lucene, so a data driver that
     * loads {@code filtered} does it only to ship it. The data drivers must load the sort key alone.
     */
    public void testNoKeepFilterOnProjected() throws Exception {
        setupIndex();
        try (var result = sendQuery("from test | where filtered > 0 | sort sorted desc | limit 3")) {
            assertThat(result.isRunning(), equalTo(false));
            assertThat(result.isPartial(), equalTo(false));
            assertSingleKeyFieldExtracted(result, "data", Set.of("sorted"));
            assertSingleKeyFieldExtracted(result, "node_reduce", Set.of("read", "filtered", "more", "some_more"));
            // The reduce driver reads these from _doc after its own TopN. The profile assertions above also pass when it reads them
            // for the wrong rows, so check that each value sits next to its sort key.
            assertThat(column(result, "sorted"), equalTo(List.of(2046L, 2044L, 2042L)));
            assertThat(column(result, "read"), equalTo(List.of(1023L, 1022L, 1021L)));
            assertThat(column(result, "filtered"), equalTo(List.of(3069L, 3066L, 3063L)));
        }
    }

    /**
     * A {@code FORK} branch has no narrowing {@code KEEP}: {@code ProjectAwayColumns} keeps everything {@code MergeExec} needs. The
     * data drivers of each branch must still load only the sort key.
     *
     * <p>The drivers of both branches report the same {@code "data"} and {@code "node_reduce"} descriptions, so each assertion
     * covers both branches. The {@code node_reduce} assertion shows that the branches defer the other fields instead of not
     * loading them.
     */
    public void testForkBranchesLateMaterialize() throws Exception {
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
     * The data drivers cannot push {@code filtered % 7 >= 0} to Lucene, so they load {@code filtered} for every row. It still does
     * not cross the exchange: the node-reduce driver loads it again for the rows that survive. The data drivers have one reader
     * for the filter and one for the sort key, so only the node-reduce driver is checked.
     */
    public void testNoKeepNonPushableFilter() throws Exception {
        setupIndex();
        try (var result = sendQuery("from test | where filtered % 7 >= 0 | sort sorted desc | limit 3")) {
            assertThat(result.isRunning(), equalTo(false));
            assertThat(result.isPartial(), equalTo(false));
            assertSingleKeyFieldExtracted(result, "node_reduce", Set.of("read", "filtered", "more", "some_more"));
            assertThat(column(result, "sorted"), equalTo(List.of(2046L, 2044L, 2042L)));
            assertThat(column(result, "filtered"), equalTo(List.of(3069L, 3066L, 3063L)));
        }
    }

    /**
     * The {@code EVAL} under the TopN reads {@code _id} and {@code _index}, and the query returns them. The data drivers load
     * them for the {@code EVAL}, and the node-reduce driver loads them again for the rows that survive. The data drivers have one
     * reader for the {@code EVAL} and one for the sort key, so only the node-reduce driver is checked.
     */
    public void testMetadataReadBelowTopNIsReloaded() throws Exception {
        setupIndex();
        String query = """
            from test metadata _id, _index
            | eval e = concat(_index, "/", _id)
            | where some_more > 0
            | sort sorted desc
            | limit 3
            """;
        try (var result = sendQuery(query)) {
            assertThat(result.isRunning(), equalTo(false));
            assertThat(result.isPartial(), equalTo(false));
            assertNodeReduceLoads(result, "_id", "_index");
            List<Object> ids = column(result, "_id");
            assertThat(ids, equalTo(List.of("1023", "1022", "1021")));
            assertThat(column(result, "_index"), equalTo(List.of("test", "test", "test")));
            assertThat(column(result, "e"), equalTo(ids.stream().<Object>map(id -> "test/" + id).toList()));
            assertThat(column(result, "sorted"), equalTo(List.of(2046L, 2044L, 2042L)));
            assertThat(column(result, "read"), equalTo(List.of(1023L, 1022L, 1021L)));
        }
    }

    /**
     * A {@code FORK} branch with a {@code LOOKUP JOIN} under its TopN. {@code animal} is a field of the lookup index, which has no
     * {@code _doc} on the node-reduce driver, so it crosses the exchange. The data drivers of the first branch have one reader
     * for the join key and one for the sort key, so only the node-reduce drivers are checked.
     */
    public void testForkBranchWithLookupJoin() throws Exception {
        setupIndex();
        client().admin()
            .indices()
            .prepareCreate("test-lookup")
            .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.mode", "lookup"))
            .setMapping("read", "type=long", "animal", "type=keyword")
            .get();
        indexRandom(
            true,
            prepareIndex("test-lookup").setSource("read", 1023, "animal", "fox"),
            prepareIndex("test-lookup").setSource("read", 1022, "animal", "dog"),
            prepareIndex("test-lookup").setSource("read", 1021, "animal", "cat")
        );
        String query = """
            from test
            | fork ( lookup join test-lookup on read | where filtered > 0 | sort sorted desc | limit 3 )
                   ( where more > 0 | sort sorted desc | limit 3 )
            | sort _fork, sorted desc
            """;
        try (var result = sendQuery(query)) {
            assertThat(result.isRunning(), equalTo(false));
            assertThat(result.isPartial(), equalTo(false));
            assertSingleKeyFieldExtracted(result, "node_reduce", Set.of("read", "filtered", "more", "some_more"));
            assertThat(column(result, "sorted"), equalTo(List.of(2046L, 2044L, 2042L, 2046L, 2044L, 2042L)));
            assertThat(column(result, "read"), equalTo(List.of(1023L, 1022L, 1021L, 1023L, 1022L, 1021L)));
            assertThat(column(result, "animal"), equalTo(Arrays.asList("fox", "dog", "cat", null, null, null)));
        }
    }

    /**
     * {@code SORT ... | LIMIT n BY} without a narrowing {@code KEEP}. The data drivers cannot push {@code payload % 7 >= 0} to
     * Lucene, so they load {@code payload}, and the node-reduce driver loads it again for the rows that survive. Each
     * {@code payload} must sit next to its own sort key.
     */
    public void testLimitByValues() throws Exception {
        assumeTrue("requires SORT | LIMIT BY", EsqlCapabilities.Cap.ESQL_TOPN_BY.isEnabled());
        client().admin()
            .indices()
            .prepareCreate("limit_by")
            .setSettings(indexSettings(10, 0))
            .setMapping("grp", "type=long", "sorted", "type=long", "payload", "type=long")
            .get();
        indexRandom(
            true,
            IntStream.range(0, 100)
                .mapToObj(i -> prepareIndex("limit_by").setId(Integer.toString(i)).setSource("grp", i % 3, "sorted", i, "payload", i * 10))
                .toList()
        );
        try (var result = sendQuery("from limit_by | where payload % 7 >= 0 | sort sorted desc | limit 2 by grp | sort sorted desc")) {
            assertThat(result.isRunning(), equalTo(false));
            assertThat(result.isPartial(), equalTo(false));
            assertNodeReduceLoads(result, "payload");
            assertThat(column(result, "sorted"), equalTo(List.of(99L, 98L, 97L, 96L, 95L, 94L)));
            assertThat(column(result, "grp"), equalTo(List.of(0L, 2L, 1L, 0L, 2L, 1L)));
            assertThat(column(result, "payload"), equalTo(List.of(990L, 980L, 970L, 960L, 950L, 940L)));
        }
    }

    /**
     * {@code u} is a {@code long} in one index and a {@code keyword} in the other. The {@code WHERE} under the TopN reads the
     * converted field and the query returns it. {@code Fetchability} does not accept union types, so the converted values cross
     * the exchange and the node-reduce driver does not load {@code u}.
     */
    public void testUnionTypeReadBelowTopNCrossesTheExchange() throws Exception {
        client().admin()
            .indices()
            .prepareCreate("union_long")
            .setSettings(indexSettings(shardCount, 0))
            .setMapping("u", "type=long", "sorted", "type=long", "read", "type=long")
            .get();
        client().admin()
            .indices()
            .prepareCreate("union_keyword")
            .setSettings(indexSettings(shardCount, 0))
            .setMapping("u", "type=keyword", "sorted", "type=long", "read", "type=long")
            .get();
        var builders = new ArrayList<IndexRequestBuilder>();
        for (int i = 0; i < 50; i++) {
            builders.add(prepareIndex("union_long").setSource("u", i, "sorted", 2 * i, "read", i));
            builders.add(prepareIndex("union_keyword").setSource("u", "k" + i, "sorted", 2 * i + 1, "read", -i));
        }
        indexRandom(true, builders);
        String query = """
            from union_long, union_keyword
            | eval s = u::keyword
            | where length(s) > 0
            | sort sorted desc
            | limit 3
            | keep sorted, s, read
            """;
        try (var result = sendQuery(query)) {
            assertThat(result.isRunning(), equalTo(false));
            assertThat(result.isPartial(), equalTo(false));
            assertSingleKeyFieldExtracted(result, "node_reduce", Set.of("read"));
            assertThat(column(result, "sorted"), equalTo(List.of(99L, 98L, 97L)));
            assertThat(column(result, "s"), equalTo(List.of("k49", "49", "k48")));
            assertThat(column(result, "read"), equalTo(List.of(-49L, 49L, -48L)));
        }
    }

    /** Asserts that the node-reduce drivers build a reader for each of the fields. */
    private static void assertNodeReduceLoads(EsqlQueryResponse response, String... fields) {
        Set<String> nodeReduceReaders = response.profile()
            .drivers()
            .stream()
            .filter(d -> d.description().equals("node_reduce"))
            .flatMap(d -> d.operators().stream())
            .filter(o -> o.operator().startsWith("ValuesSourceReaderOperator"))
            .flatMap(o -> ((ValuesSourceReaderOperatorStatus) o.status()).readersBuilt().keySet().stream())
            .collect(Collectors.toSet());
        for (String name : fields) {
            assertTrue(
                "node_reduce does not load [" + name + "]: " + nodeReduceReaders,
                nodeReduceReaders.stream().anyMatch(r -> r.contains(name))
            );
        }
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
