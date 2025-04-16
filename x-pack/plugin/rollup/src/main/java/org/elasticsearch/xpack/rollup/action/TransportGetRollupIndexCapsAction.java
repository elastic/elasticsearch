/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.xpack.core.rollup.action.GetRollupIndexCapsAction;
import org.elasticsearch.xpack.core.rollup.action.RollableIndexCaps;
import org.elasticsearch.xpack.core.rollup.action.RollupJobCaps;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.rollup.Rollup.DEPRECATION_KEY;
import static org.elasticsearch.xpack.rollup.Rollup.DEPRECATION_MESSAGE;

public class TransportGetRollupIndexCapsAction extends HandledTransportAction<
    GetRollupIndexCapsAction.Request,
    GetRollupIndexCapsAction.Response> {

    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(TransportGetRollupCapsAction.class);

    private final ClusterService clusterService;
    private final IndexNameExpressionResolver resolver;
    private final Executor managementExecutor;

    @Inject
    public TransportGetRollupIndexCapsAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        // TODO replace SAME when removing workaround for https://github.com/elastic/elasticsearch/issues/97916
        super(
            GetRollupIndexCapsAction.NAME,
            transportService,
            actionFilters,
            GetRollupIndexCapsAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.clusterService = clusterService;
        this.managementExecutor = transportService.getThreadPool().executor(ThreadPool.Names.MANAGEMENT);
        this.resolver = indexNameExpressionResolver;
    }

    @Override
    protected void doExecute(
        Task task,
        GetRollupIndexCapsAction.Request request,
        ActionListener<GetRollupIndexCapsAction.Response> listener
    ) {
        DEPRECATION_LOGGER.warn(DeprecationCategory.API, DEPRECATION_KEY, DEPRECATION_MESSAGE);
        // Workaround for https://github.com/elastic/elasticsearch/issues/97916 - TODO remove this when we can
        managementExecutor.execute(ActionRunnable.wrap(listener, l -> doExecuteForked(request, l)));
    }

    private void doExecuteForked(IndicesRequest request, ActionListener<GetRollupIndexCapsAction.Response> listener) {
        Transports.assertNotTransportThread("retrieving rollup job index caps may be expensive");
        String[] indices = resolver.concreteIndexNames(clusterService.state(), request.indicesOptions(), request);
        Map<String, RollableIndexCaps> allCaps = getCapsByRollupIndex(
            Arrays.asList(indices),
            clusterService.state().getMetadata().getProject().indices()
        );
        listener.onResponse(new GetRollupIndexCapsAction.Response(allCaps));
    }

    static Map<String, RollableIndexCaps> getCapsByRollupIndex(List<String> resolvedIndexNames, Map<String, IndexMetadata> indices) {
        Map<String, List<RollupJobCaps>> allCaps = new TreeMap<>();

        indices.entrySet().stream().filter(entry -> resolvedIndexNames.contains(entry.getKey())).forEach(entry -> {
            // Does this index have rollup metadata?
            TransportGetRollupCapsAction.findRollupIndexCaps(entry.getKey(), entry.getValue()).ifPresent(cap -> {
                cap.getJobCaps().forEach(jobCap -> {
                    // Do we already have an entry for this index?
                    List<RollupJobCaps> indexCaps = allCaps.get(jobCap.getRollupIndex());
                    if (indexCaps == null) {
                        indexCaps = new ArrayList<>();
                    }
                    indexCaps.add(jobCap);
                    allCaps.put(jobCap.getRollupIndex(), indexCaps);
                });
            });
        });
        // Convert the mutable lists into the RollableIndexCaps
        return allCaps.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> new RollableIndexCaps(e.getKey(), e.getValue())));
    }

}
