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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.SerializationTestUtils;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.session.Versioned;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomConfiguration;
import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomTables;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.containsString;

public class DataNodeRequestSerializationTests extends AbstractWireSerializingTestCase<DataNodeRequest> {
    @Override
    protected Writeable.Reader<DataNodeRequest> instanceReader() {
        return in -> new DataNodeRequest(in, new SerializationTestUtils.TestNameIdMapper());
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> writeables = new ArrayList<>();
        writeables.addAll(new SearchModule(Settings.EMPTY, List.of()).getNamedWriteables());
        writeables.addAll(new EsqlPlugin().getNamedWriteables());
        return new NamedWriteableRegistry(writeables);
    }

    @Override
    protected DataNodeRequest createTestInstance() {
        var sessionId = randomAlphaOfLength(10);
        String query = randomFrom("""
            from test
            | where round(emp_no) > 10
            | eval c = salary
            | stats x = avg(c)
            """, """
            from test
            | sort last_name
            | limit 10
            | where round(emp_no) > 10
            | eval c = first_name
            | stats x = avg(salary)
            """);
        List<DataNodeRequest.Shard> shards = randomList(
            1,
            10,
            () -> new DataNodeRequest.Shard(
                new ShardId("index-" + between(1, 10), "n/a", between(1, 10)),
                SplitShardCountSummary.fromInt(randomIntBetween(0, 1024))
            )
        );
        PhysicalPlan physicalPlan = mapAndMaybeOptimize(parse(query));
        Map<Index, AliasFilter> aliasFilters = Map.of(
            new Index("concrete-index", "n/a"),
            AliasFilter.of(new TermQueryBuilder("id", "1"), "alias-1")
        );
        DataNodeRequest request = new DataNodeRequest(
            sessionId,
            randomConfiguration(query, randomTables()),
            randomAlphaOfLength(10),
            shards,
            aliasFilters,
            physicalPlan,
            generateRandomStringArray(10, 10, false, false),
            IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()),
            randomBoolean(),
            randomBoolean(),
            randomBoolean()
        );
        request.setParentTask(randomAlphaOfLength(10), randomNonNegativeLong());
        return request;
    }

    public void testRetainSearchContextsRoundTrips() throws IOException {
        DataNodeRequest request = createTestInstance();
        request = new DataNodeRequest(
            request.sessionId(),
            request.configuration(),
            request.clusterAlias(),
            request.shards(),
            request.aliasFilters(),
            request.plan(),
            request.indices(),
            request.indicesOptions(),
            request.runNodeLevelReduction(),
            request.reductionLateMaterialization(),
            true
        );
        request.setParentTask(randomAlphaOfLength(10), randomNonNegativeLong());

        DataNodeRequest copy = copyInstance(request, TransportVersion.current());

        assertTrue(copy.retainSearchContexts());
        assertThat(copy.getDescription(), containsString("retainSearchContexts=true"));

        DataNodeRequest downgraded = copyInstance(
            request,
            TransportVersionUtils.getPreviousVersion(DataNodeRequest.ESQL_REMOTE_FETCH_RETAINED_CONTEXTS)
        );
        assertFalse(downgraded.retainSearchContexts());
    }

    @Override
    protected DataNodeRequest mutateInstance(DataNodeRequest in) throws IOException {
        var sessionId = in.sessionId();
        var configuration = in.configuration();
        var clusterAlias = in.clusterAlias();
        var shards = in.shards();
        var aliasFilters = in.aliasFilters();
        var plan = in.plan();
        var indices = in.indices();
        var indicesOptions = in.indicesOptions();
        var runNodeLevelReduction = in.runNodeLevelReduction();
        var reductionLateMaterialization = in.reductionLateMaterialization();
        var retainSearchContexts = in.retainSearchContexts();
        TaskId parentTask = in.getParentTask();

        switch (between(0, 10)) {
            case 0 -> sessionId = randomValueOtherThan(sessionId, () -> randomAlphaOfLength(20));
            case 1 -> configuration = randomValueOtherThan(configuration, () -> randomConfiguration());
            case 2 -> shards = randomValueOtherThan(
                shards,
                () -> randomList(
                    1,
                    10,
                    () -> new DataNodeRequest.Shard(
                        new ShardId("new-index-" + between(1, 10), "n/a", between(1, 10)),
                        SplitShardCountSummary.fromInt(randomIntBetween(0, 1024))
                    )
                )
            );
            case 3 -> {
                String newQuery = randomFrom("""
                    from test
                    | where round(emp_no) > 100
                    | eval c = salary
                    | stats x = avg(c)
                    """, """
                    from test
                    | sort last_name
                    | limit 10
                    | where round(emp_no) > 100
                    | eval c = first_name
                    | stats x = avg(salary)
                    """);
                var previousPlan = plan;
                plan = randomValueOtherThan(previousPlan, () -> mapAndMaybeOptimize(parse(newQuery)));
            }
            case 4 -> aliasFilters = aliasFilters.isEmpty()
                ? Map.of(new Index("concrete-index", "n/a"), AliasFilter.of(new TermQueryBuilder("id", "2"), "alias-2"))
                : Map.of();
            case 5 -> parentTask = new TaskId(
                randomValueOtherThan(parentTask.getNodeId(), () -> randomAlphaOfLength(10)),
                randomNonNegativeLong()
            );
            case 6 -> clusterAlias = randomValueOtherThan(clusterAlias, () -> randomAlphaOfLength(10));
            case 7 -> indices = randomArrayOtherThan(indices, () -> generateRandomStringArray(10, 10, false, false));
            case 8 -> indicesOptions = randomValueOtherThan(
                indicesOptions,
                () -> IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean())
            );
            case 9 -> {
                runNodeLevelReduction = runNodeLevelReduction == false;
                reductionLateMaterialization = reductionLateMaterialization == false;
            }
            case 10 -> retainSearchContexts = retainSearchContexts == false;
            default -> throw new AssertionError("invalid value");
        }

        var request = new DataNodeRequest(
            sessionId,
            configuration,
            clusterAlias,
            shards,
            aliasFilters,
            plan,
            indices,
            indicesOptions,
            runNodeLevelReduction,
            reductionLateMaterialization,
            retainSearchContexts
        );
        request.setParentTask(parentTask);
        return request;
    }

    static Versioned<LogicalPlan> parse(String query) {
        var analyzer = analyzer().addIndex("test", "mapping-basic.json").buildAnalyzer();
        TransportVersion minimumVersion = analyzer.context().minimumVersion();
        var logicalOptimizer = new LogicalPlanOptimizer(new LogicalOptimizerContext(TEST_CFG, FoldContext.small(), minimumVersion));
        return new Versioned<>(logicalOptimizer.optimize(analyzer.analyze(TEST_PARSER.parseQuery(query))), minimumVersion);
    }

    static PhysicalPlan mapAndMaybeOptimize(Versioned<LogicalPlan> logicalPlan) {
        var physicalPlanOptimizer = new PhysicalPlanOptimizer(new PhysicalOptimizerContext(TEST_CFG, logicalPlan.minimumVersion()));
        var mapper = new Mapper();
        var physical = mapper.map(logicalPlan);
        if (randomBoolean()) {
            physical = physicalPlanOptimizer.optimize(physical);
        }
        return physical;
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
