/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.Experimental;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.xpack.esql.analyzer.Analyzer;
import org.elasticsearch.xpack.esql.analyzer.Avg;
import org.elasticsearch.xpack.esql.compute.transport.ComputeAction2;
import org.elasticsearch.xpack.esql.compute.transport.ComputeRequest2;
import org.elasticsearch.xpack.esql.plan.physical.Optimizer;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.physical.Mapper;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.ql.analyzer.PreAnalyzer;
import org.elasticsearch.xpack.ql.analyzer.TableInfo;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.index.IndexResolver;
import org.elasticsearch.xpack.ql.index.RemoteClusterResolver;
import org.elasticsearch.xpack.ql.plan.TableIdentifier;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.session.Configuration;
import org.elasticsearch.xpack.ql.type.DefaultDataTypeRegistry;
import org.junit.Assert;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;

@Experimental
@ESIntegTestCase.ClusterScope(scope = SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class ComputeEngineIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(EsqlPlugin.class);
    }

    public void testComputeEngine() {
        ElasticsearchAssertions.assertAcked(
            client().admin()
                .indices()
                .prepareCreate("test")
                .setSettings(Settings.builder().put("index.number_of_shards", ESTestCase.randomIntBetween(1, 5)))
                .get()
        );
        for (int i = 0; i < 10; i++) {
            client().prepareBulk()
                .add(new IndexRequest("test").id("1" + i).source("data", 1, "count", 42))
                .add(new IndexRequest("test").id("2" + i).source("data", 2, "count", 44))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        }
        ensureYellow("test");

        Tuple<List<ColumnInfo>, List<Page>> results = run("from test | stats avg(count)");
        logger.info(results);
        Assert.assertEquals(1, results.v1().size());
        Assert.assertEquals(1, results.v2().size());
        assertEquals("avg(count)", results.v1().get(0).name());
        assertEquals("double", results.v1().get(0).type());
        assertEquals(1, results.v2().get(0).getBlockCount());
        assertEquals(43, results.v2().get(0).getBlock(0).getDouble(0), 1d);

        results = run("from test");
        logger.info(results);
        Assert.assertEquals(20, results.v2().stream().mapToInt(Page::getPositionCount).sum());

        results = run("from test | sort count | limit 1");
        logger.info(results);
        Assert.assertEquals(1, results.v2().stream().mapToInt(Page::getPositionCount).sum());
        assertEquals(42, results.v2().get(0).getBlock(results.v1().indexOf(new ColumnInfo("count", "long"))).getLong(0));

        results = run("from test | eval x = count + 7 | sort x | limit 1");
        logger.info(results);
        Assert.assertEquals(1, results.v2().stream().mapToInt(Page::getPositionCount).sum());
        assertEquals(49, results.v2().get(0).getBlock(results.v1().indexOf(new ColumnInfo("x", "long"))).getLong(0));

        results = run("from test | stats avg_count = avg(count) | eval x = avg_count + 7");
        logger.info(results);
        Assert.assertEquals(1, results.v2().size());
        assertEquals(2, results.v2().get(0).getBlockCount());
        assertEquals(50, results.v2().get(0).getBlock(results.v1().indexOf(new ColumnInfo("x", "double"))).getDouble(0), 1d);
    }

    private Tuple<List<ColumnInfo>, List<Page>> run(String esqlCommands) {
        EsqlParser parser = new EsqlParser();
        LogicalPlan logicalPlan = parser.createStatement(esqlCommands);
        logger.info("Plan after parsing:\n{}", logicalPlan);

        PreAnalyzer.PreAnalysis preAnalysis = new PreAnalyzer().preAnalyze(logicalPlan);
        RemoteClusterResolver remoteClusterResolver = new RemoteClusterResolver(Settings.EMPTY, clusterService().getClusterSettings());
        IndexResolver indexResolver = new IndexResolver(
            client(),
            clusterService().getClusterName().value(),
            DefaultDataTypeRegistry.INSTANCE,
            remoteClusterResolver::remoteClusters
        );
        if (preAnalysis.indices.size() != 1) {
            throw new UnsupportedOperationException();
        }
        TableInfo tableInfo = preAnalysis.indices.get(0);
        TableIdentifier table = tableInfo.id();

        PlainActionFuture<IndexResolution> fut = new PlainActionFuture<>();
        indexResolver.resolveAsMergedMapping(table.index(), false, Map.of(), fut);
        FunctionRegistry functionRegistry = new FunctionRegistry(FunctionRegistry.def(Avg.class, Avg::new, "AVG"));
        Configuration configuration = new Configuration(ZoneOffset.UTC, null, null, x -> Collections.emptySet());
        Analyzer analyzer = new Analyzer(fut.actionGet(), functionRegistry, configuration);
        logicalPlan = analyzer.analyze(logicalPlan);
        logger.info("Plan after analysis:\n{}", logicalPlan);
        Mapper mapper = new Mapper();
        PhysicalPlan physicalPlan = mapper.map(logicalPlan);
        Optimizer optimizer = new Optimizer();
        physicalPlan = optimizer.optimize(physicalPlan);
        logger.info("Physical plan after optimize:\n{}", physicalPlan);

        List<ColumnInfo> columns = physicalPlan.output()
            .stream()
            .map(c -> new ColumnInfo(c.qualifiedName(), c.dataType().esType()))
            .toList();

        return Tuple.tuple(columns, client().execute(ComputeAction2.INSTANCE, new ComputeRequest2(physicalPlan)).actionGet().getPages());
    }
}
