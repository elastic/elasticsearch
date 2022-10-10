/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.Experimental;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.xpack.esql.compute.transport.ComputeAction;
import org.elasticsearch.xpack.esql.compute.transport.ComputeRequest;
import org.elasticsearch.xpack.esql.plan.physical.old.PlanNode;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.junit.Assert;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

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
            ESIntegTestCase.client()
                .admin()
                .indices()
                .prepareCreate("test")
                .setSettings(Settings.builder().put("index.number_of_shards", ESTestCase.randomIntBetween(1, 5)))
                .get()
        );
        for (int i = 0; i < 10; i++) {
            ESIntegTestCase.client()
                .prepareBulk()
                .add(new IndexRequest("test").id("1" + i).source("data", "bar", "count", 42))
                .add(new IndexRequest("test").id("2" + i).source("data", "baz", "count", 44))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        }
        ensureYellow("test");

        List<Page> pages = ESIntegTestCase.client()
            .execute(
                ComputeAction.INSTANCE,
                new ComputeRequest(
                    PlanNode.builder(new MatchAllDocsQuery(), randomFrom(PlanNode.LuceneSourceNode.Parallelism.values()), "test")
                        .numericDocValues("count")
                        .avgPartial("count")
                        .exchange(PlanNode.ExchangeNode.Type.GATHER, PlanNode.ExchangeNode.Partitioning.SINGLE_DISTRIBUTION)
                        .avgFinal("count")
                        .buildWithoutOutputNode()
                )
            )
            .actionGet()
            .getPages();
        logger.info(pages);
        Assert.assertEquals(1, pages.size());
        assertEquals(1, pages.get(0).getBlockCount());
        assertEquals(43, pages.get(0).getBlock(0).getDouble(0), 0.1d);

        pages = ESIntegTestCase.client()
            .execute(
                ComputeAction.INSTANCE,
                new ComputeRequest(
                    PlanNode.builder(new MatchAllDocsQuery(), randomFrom(PlanNode.LuceneSourceNode.Parallelism.values()), "test")
                        .numericDocValues("count")
                        .exchange(PlanNode.ExchangeNode.Type.GATHER, PlanNode.ExchangeNode.Partitioning.SINGLE_DISTRIBUTION)
                        .buildWithoutOutputNode()
                )
            )
            .actionGet()
            .getPages();
        logger.info(pages);
        Assert.assertEquals(20, pages.stream().mapToInt(Page::getPositionCount).sum());
    }
}
