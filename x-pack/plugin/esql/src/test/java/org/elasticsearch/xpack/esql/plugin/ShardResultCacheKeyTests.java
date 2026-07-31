/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.Versioned;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * The shared part of the cache key has to be a function of what a query computes and nothing else. These test that two
 * independent plannings of the same query agree, and that the things which do change the rows disagree.
 */
public class ShardResultCacheKeyTests extends ESTestCase {

    private static final EsqlFlags FLAGS = new EsqlFlags(true, 127);

    private static final String AGGREGATION = """
        from test
        | where emp_no > 10
        | stats x = avg(salary) by languages
        """;

    public void testTwoPlanningsOfTheSameQueryAgree() throws Exception {
        // Each planning allocates fresh NameIds from a process-global counter, which is the main thing the key has to
        // see through.
        assertThat(digest(request(AGGREGATION)), equalTo(digest(request(AGGREGATION))));
    }

    public void testSourcePositionsDoNotReachTheKey() throws Exception {
        // The same query written with different whitespace is the same computation at a different offset. A prepared
        // statement arriving without its text is the same case, and is what makes this more than cosmetic.
        String reformatted = "from test | where emp_no > 10       | stats x = avg(salary) by languages";
        assertThat(digest(request(AGGREGATION)), equalTo(digest(request(reformatted))));
    }

    public void testADifferentFilterIsADifferentKey() throws Exception {
        String other = """
            from test
            | where emp_no > 11
            | stats x = avg(salary) by languages
            """;
        assertThat(digest(request(AGGREGATION)), not(equalTo(digest(request(other)))));
    }

    public void testADifferentAggregationIsADifferentKey() throws Exception {
        String other = """
            from test
            | where emp_no > 10
            | stats x = max(salary) by languages
            """;
        assertThat(digest(request(AGGREGATION)), not(equalTo(digest(request(other)))));
    }

    public void testADifferentGroupingIsADifferentKey() throws Exception {
        String other = """
            from test
            | where emp_no > 10
            | stats x = avg(salary) by first_name
            """;
        assertThat(digest(request(AGGREGATION)), not(equalTo(digest(request(other)))));
    }

    public void testAliasFilterIsPartOfTheKey() throws Exception {
        DataNodeRequest plain = request(AGGREGATION);
        DataNodeRequest filtered = withAliasFilters(
            plain,
            Map.of(new Index("test", "test-uuid"), AliasFilter.of(QueryBuilders.termQuery("gender", "F"), "females"))
        );
        assertThat(digest(plain), not(equalTo(digest(filtered))));
    }

    public void testClusterAliasIsPartOfTheKey() throws Exception {
        DataNodeRequest local = request(AGGREGATION);
        DataNodeRequest remote = new DataNodeRequest(
            local.sessionId(),
            local.configuration(),
            "remote1",
            local.shards(),
            local.aliasFilters(),
            local.plan(),
            local.indices(),
            local.indicesOptions(),
            local.runNodeLevelReduction(),
            local.reductionLateMaterialization(),
            local.retainSearchContexts()
        );
        assertThat(digest(local), not(equalTo(digest(remote))));
    }

    /**
     * The reduction split picks what the node-reduce driver does, downstream of the entry, and the data-node half of
     * the split is what the caller hands to the key. Keying the flag too would split one shard's entry in two depending
     * on whether the coordinator happened to be this node.
     */
    public void testTheReductionSplitIsNotPartOfTheKey() throws Exception {
        DataNodeRequest reducing = request(AGGREGATION);
        DataNodeRequest notReducing = new DataNodeRequest(
            reducing.sessionId(),
            reducing.configuration(),
            reducing.clusterAlias(),
            reducing.shards(),
            reducing.aliasFilters(),
            reducing.plan(),
            reducing.indices(),
            reducing.indicesOptions(),
            reducing.runNodeLevelReduction() == false,
            reducing.reductionLateMaterialization(),
            reducing.retainSearchContexts()
        );
        assertThat(digest(reducing), equalTo(digest(notReducing)));
    }

    /**
     * A relative-time window ships as a folded literal, so a dashboard refreshing every minute would otherwise produce
     * a fresh key every minute. The bounds come out of the shared digest and are resolved per shard instead.
     */
    public void testTimePredicateBoundsAreLiftedOutOfTheDigest() throws Exception {
        String early = "from test | where hire_date >= \"2024-01-01T00:00:00Z\" | stats x = avg(salary)";
        String late = "from test | where hire_date >= \"2024-06-01T00:00:00Z\" | stats x = avg(salary)";
        ShardResultCacheKey.QueryPart earlyPart = ShardResultCacheKey.queryPart(request(early), FLAGS);
        ShardResultCacheKey.QueryPart latePart = ShardResultCacheKey.queryPart(request(late), FLAGS);
        assertThat(earlyPart.liftedRanges().size(), equalTo(1));
        assertThat(earlyPart.liftedRanges().getFirst().fieldName(), equalTo("hire_date"));
        assertThat(latePart.liftedRanges().getFirst().from(), not(equalTo(earlyPart.liftedRanges().getFirst().from())));
        assertThat(earlyPart.digest(), equalTo(latePart.digest()));
    }

    /**
     * The wire plan is planned again on the data node under these flags, and both of them can change which rows come
     * out, so the same plan under different flags has to be a different entry.
     */
    public void testNodeFlagsArePartOfTheKey() throws Exception {
        DataNodeRequest request = request(AGGREGATION);
        byte[] digest = ShardResultCacheKey.queryPart(request, FLAGS).digest();
        assertThat(digest, not(equalTo(ShardResultCacheKey.queryPart(request, new EsqlFlags(false, 127)).digest())));
        assertThat(digest, not(equalTo(ShardResultCacheKey.queryPart(request, new EsqlFlags(true, -1)).digest())));
    }

    /**
     * A node request handled on the coordinator's own node arrives as the very plan objects the coordinator built,
     * while every other node reads a copy off the wire. The two have to key the same, or a shard's entry would be
     * found only on the runs where its node happened to play the same role as when the entry was written.
     */
    public void testAWirePlanKeysTheSameAsTheOneItWasCopiedFrom() throws Exception {
        List<NamedWriteableRegistry.Entry> writeables = new ArrayList<>(new SearchModule(Settings.EMPTY, List.of()).getNamedWriteables());
        writeables.addAll(new EsqlPlugin().getNamedWriteables());
        NamedWriteableRegistry registry = new NamedWriteableRegistry(writeables);
        for (String query : List.of(
            AGGREGATION,
            "from test | stats c = count(*)",
            "from test | stats x = sum(salary) by first_name",
            "from test | where first_name == \"Bob\" | stats x = max(salary)",
            "from test | where hire_date >= \"2024-01-01T00:00:00Z\" | stats x = avg(salary)"
        )) {
            DataNodeRequest local = request(query);
            DataNodeRequest remote = copyWriteable(local, registry, DataNodeRequest::new, TransportVersion.current());
            assertThat(query, digest(remote), equalTo(digest(local)));
        }
    }

    /** A predicate over a different field is a different query even once both are lifted. */
    public void testALiftedPredicateStillNamesItsField() throws Exception {
        String onHireDate = "from test | where hire_date >= \"2024-01-01T00:00:00Z\" | stats x = avg(salary)";
        String unfiltered = "from test | stats x = avg(salary)";
        assertThat(digest(request(onHireDate)), not(equalTo(digest(request(unfiltered)))));
    }

    /**
     * The key is default-deny by intent, but nothing enforces that on a field added later: an author who adds a
     * {@link Configuration} field and forgets this class gets wrong rows, not a lower hit rate. Failing here is the
     * enforcement. A new field belongs in {@code KEYED} unless it demonstrably cannot change what a shard produces, in
     * which case it belongs in {@code NOT_KEYED} with the reason recorded in
     * {@code ShardResultCacheKey#writeConfiguration}.
     */
    public void testEveryConfigurationFieldHasAVerdict() {
        Set<String> keyed = Set.of(
            "clusterName",
            "username",
            "locale",
            "pragmas",
            "resultTruncationMaxSizeRegular",
            "resultTruncationDefaultSizeRegular",
            "resultTruncationMaxSizeTimeseries",
            "resultTruncationDefaultSizeTimeseries",
            "allowPartialResults",
            "tables",
            "resolvedSettings",
            "viewQueries"
        );
        Set<String> notKeyed = Set.of("now", "queryStartTimeNanos", "query", "profile", "explainOnly");
        assertFieldsHaveVerdicts(Configuration.class, keyed, notKeyed);
    }

    /** The same guard over the node flags, every one of which drives the local replan of the wire plan. */
    public void testEveryNodeFlagHasAVerdict() {
        assertFieldsHaveVerdicts(EsqlFlags.class, Set.of("stringLikeOnIndex", "roundToPushdownThreshold"), Set.of());
    }

    /**
     * {@link PlannerSettings} members pick slice boundaries, buffer sizes, and TopN caps. They change how rows flow
     * through the engine rather than which rows a shard produces, and the verifier refuses the shapes (row-returning,
     * sorted) whose correctness would be affected by them. Any new member therefore belongs here with the same verdict,
     * unless it demonstrably changes which rows a shard returns — in which case it belongs in {@link EsqlFlags} or in
     * the node flags written by {@code ShardResultCacheKey#writeFlags}.
     */
    public void testEveryPlannerSettingsFieldHasAVerdict() {
        Set<String> notKeyed = Set.of(
            "defaultDataPartitioning",
            "docsThresholdForAutoPartitioning",
            "valuesLoadingJumboSize",
            "luceneTopNLimit",
            "intermediateLocalRelationMaxSize",
            "partialEmitKeysThreshold",
            "partialEmitUniquenessThreshold",
            "timeSeriesTargetChunkRows",
            "reuseColumnLoadersThreshold",
            "blockLoaderSizeOrdinals",
            "blockLoaderSizeScript",
            "maxKeywordSortFields",
            "sourceReservationFactor",
            "bytesRefRamOverestimateThreshold",
            "bytesRefRamOverestimateFactor",
            "docSequenceBytesRefFieldThreshold",
            "parallelTopNPromotionThresholdRows",
            "parallelTopNMaxWorkers",
            "inSubqueryHashJoinThreshold"
        );
        assertFieldsHaveVerdicts(PlannerSettings.class, Set.of(), notKeyed);
    }

    /** The same guard over the request, whose fields are the other half of what the shared digest sees. */
    public void testEveryDataNodeRequestFieldHasAVerdict() {
        Set<String> keyed = Set.of("configuration", "clusterAlias", "aliasFilters", "plan");
        /*
         * sessionId, indices and indicesOptions name the query run and how its target was resolved, not what a shard
         * computes; shards is per shard and enters through forShard; the three split flags are covered by
         * testTheReductionSplitIsNotPartOfTheKey; externalSplits is refused outright by the verifier.
         */
        Set<String> notKeyed = Set.of(
            "sessionId",
            "shards",
            "indices",
            "indicesOptions",
            "runNodeLevelReduction",
            "reductionLateMaterialization",
            "retainSearchContexts",
            "externalSplits"
        );
        assertFieldsHaveVerdicts(DataNodeRequest.class, keyed, notKeyed);
    }

    @SuppressForbidden(reason = "need access to all fields, they are mostly private")
    private static void assertFieldsHaveVerdicts(Class<?> type, Set<String> keyed, Set<String> notKeyed) {
        assertThat("a field cannot be both keyed and not", Sets.intersection(keyed, notKeyed), empty());
        Set<String> declared = Arrays.stream(type.getDeclaredFields())
            .filter(field -> Modifier.isStatic(field.getModifiers()) == false)
            .map(Field::getName)
            .collect(Collectors.toSet());
        Set<String> verdicts = Sets.union(keyed, notKeyed);
        assertThat("fields with no recorded verdict", Sets.difference(declared, verdicts), empty());
        assertThat("verdicts for fields that no longer exist", Sets.difference(verdicts, declared), empty());
    }

    private static byte[] digest(DataNodeRequest request) throws Exception {
        return ShardResultCacheKey.queryPart(request, FLAGS).digest();
    }

    static DataNodeRequest request(String query) {
        return request(query, Map.of());
    }

    /**
     * Plans {@code query} the way the coordinator does and wraps the data-node half of the split in a request.
     * Everything a test planning must not vary by is pinned: the physical optimizer always runs, and the cluster's
     * minimum transport version is the current one, because a plan is version-dependent (a {@code SUM} carries a
     * different overflow mode on an older cluster) and two plannings compared here have to be of one cluster.
     */
    static DataNodeRequest request(String query, Map<Index, AliasFilter> aliasFilters) {
        var analyzer = analyzer().addIndex("test", "mapping-basic.json")
            .minimumTransportVersion(TransportVersion.current())
            .buildAnalyzer();
        TransportVersion minimumVersion = analyzer.context().minimumVersion();
        LogicalPlan analyzed = new LogicalPlanOptimizer(new LogicalOptimizerContext(TEST_CFG, FoldContext.small(), minimumVersion))
            .optimize(analyzer.analyze(TEST_PARSER.parseQuery(query)));
        Versioned<LogicalPlan> logical = new Versioned<>(analyzed, minimumVersion);
        PhysicalPlan physical = new PhysicalPlanOptimizer(new PhysicalOptimizerContext(TEST_CFG, minimumVersion)).optimize(
            new Mapper().map(logical)
        );
        Tuple<PhysicalPlan, PhysicalPlan> split = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(physical, TEST_CFG);
        return new DataNodeRequest(
            "session",
            configuration(query),
            "",
            List.of(new DataNodeRequest.Shard(new ShardId("test", "test-uuid", 0), SplitShardCountSummary.fromInt(0))),
            aliasFilters,
            split.v2(),
            new String[] { "test" },
            IndicesOptions.STRICT_EXPAND_OPEN,
            true,
            false,
            false
        );
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    private static DataNodeRequest withAliasFilters(DataNodeRequest request, Map<Index, AliasFilter> aliasFilters) {
        return new DataNodeRequest(
            request.sessionId(),
            request.configuration(),
            request.clusterAlias(),
            request.shards(),
            aliasFilters,
            request.plan(),
            request.indices(),
            request.indicesOptions(),
            request.runNodeLevelReduction(),
            request.reductionLateMaterialization(),
            request.retainSearchContexts()
        );
    }
}
