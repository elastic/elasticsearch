/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Computes the result of a {@link PhysicalPlan}.
 */
public class ComputeService {
    private static final Logger LOGGER = LogManager.getLogger(ComputeService.class);
    private final SearchService searchService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final BigArrays bigArrays;

    public ComputeService(
        SearchService searchService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterService clusterService,
        ThreadPool threadPool,
        BigArrays bigArrays
    ) {
        this.searchService = searchService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.bigArrays = bigArrays.withCircuitBreaking();
    }

    private void acquireSearchContexts(PhysicalPlan physicalPlan, ActionListener<List<SearchContext>> listener) {
        try {
            Set<String> indexNames = physicalPlan.collect(l -> l instanceof EsQueryExec)
                .stream()
                .map(qe -> ((EsQueryExec) qe).index().name())
                .collect(Collectors.toSet());
            Index[] indices = indexNameExpressionResolver.concreteIndices(
                clusterService.state(),
                IndicesOptions.STRICT_EXPAND_OPEN,
                indexNames.toArray(String[]::new)
            );
            List<IndexShard> targetShards = new ArrayList<>();
            for (Index index : indices) {
                IndexService indexService = searchService.getIndicesService().indexServiceSafe(index);
                for (IndexShard indexShard : indexService) {
                    targetShards.add(indexShard);
                }
            }
            if (targetShards.isEmpty()) {
                listener.onResponse(List.of());
                return;
            }
            CountDown countDown = new CountDown(targetShards.size());
            for (IndexShard targetShard : targetShards) {
                targetShard.awaitShardSearchActive(ignored -> {
                    if (countDown.countDown()) {
                        ActionListener.completeWith(listener, () -> {
                            final List<SearchContext> searchContexts = new ArrayList<>();
                            boolean success = false;
                            try {
                                for (IndexShard shard : targetShards) {
                                    ShardSearchRequest shardSearchLocalRequest = new ShardSearchRequest(
                                        shard.shardId(),
                                        0,
                                        AliasFilter.EMPTY
                                    );
                                    SearchContext context = searchService.createSearchContext(
                                        shardSearchLocalRequest,
                                        SearchService.NO_TIMEOUT
                                    );
                                    searchContexts.add(context);
                                }
                                for (SearchContext searchContext : searchContexts) {
                                    searchContext.preProcess();
                                }
                                success = true;
                                return searchContexts;
                            } finally {
                                if (success == false) {
                                    IOUtils.close(searchContexts);
                                }
                            }
                        });
                    }
                });
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    public void runCompute(PhysicalPlan physicalPlan, EsqlConfiguration configuration, ActionListener<List<Page>> listener) {
        acquireSearchContexts(physicalPlan, ActionListener.wrap(searchContexts -> {
            boolean success = false;
            List<Driver> drivers = new ArrayList<>();
            CheckedRunnable<RuntimeException> release = () -> Releasables.close(
                () -> Releasables.close(searchContexts),
                () -> Releasables.close(drivers)
            );
            try {
                LocalExecutionPlanner planner = new LocalExecutionPlanner(bigArrays, configuration, searchContexts);
                final List<Page> results = Collections.synchronizedList(new ArrayList<>());
                LocalExecutionPlanner.LocalExecutionPlan localExecutionPlan = planner.plan(
                    new OutputExec(physicalPlan, (l, p) -> { results.add(p); })
                );
                LOGGER.info("Local execution plan:\n{}", localExecutionPlan.describe());
                localExecutionPlan.createDrivers(drivers);
                if (drivers.isEmpty()) {
                    throw new IllegalStateException("no drivers created");
                }
                LOGGER.info("using {} drivers", drivers.size());
                Driver.start(threadPool.executor(ThreadPool.Names.SEARCH), drivers)
                    .addListener(ActionListener.runBefore(new ActionListener<>() {
                        @Override
                        public void onResponse(Void unused) {
                            listener.onResponse(new ArrayList<>(results));
                        }

                        @Override
                        public void onFailure(Exception e) {
                            listener.onFailure(e);
                        }
                    }, release));
                success = true;
            } finally {
                if (success == false) {
                    release.run();
                }
            }
        }, listener::onFailure));
    }
}
