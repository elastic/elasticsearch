/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.rollup.action.GetRollupIndexCapsAction;
import org.elasticsearch.xpack.core.rollup.action.RollableIndexCaps;
import org.elasticsearch.xpack.core.rollup.action.RollupJobCaps;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class TransportGetRollupIndexCapsAction extends HandledTransportAction<
    GetRollupIndexCapsAction.Request,
    GetRollupIndexCapsAction.Response> {

    private final ClusterService clusterService;
    private final IndexNameExpressionResolver resolver;

    @Inject
    public TransportGetRollupIndexCapsAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetRollupIndexCapsAction.NAME,
            transportService,
            actionFilters,
            GetRollupIndexCapsAction.Request::new,
            ThreadPool.Names.MANAGEMENT
        );
        this.clusterService = clusterService;
        this.resolver = indexNameExpressionResolver;
    }

    @Override
    protected void doExecute(
        Task task,
        GetRollupIndexCapsAction.Request request,
        ActionListener<GetRollupIndexCapsAction.Response> listener
    ) {

        String[] indices = resolver.concreteIndexNames(clusterService.state(), request.indicesOptions(), request);
        Map<String, RollableIndexCaps> allCaps = getCapsByRollupIndex(
            Arrays.asList(indices),
            clusterService.state().getMetadata().indices()
        );
        listener.onResponse(new GetRollupIndexCapsAction.Response(allCaps));
    }

    static Map<String, RollableIndexCaps> getCapsByRollupIndex(
        List<String> resolvedIndexNames,
        ImmutableOpenMap<String, IndexMetadata> indices
    ) {
        Map<String, List<RollupJobCaps>> allCaps = new TreeMap<>();

        indices.stream().filter(entry -> resolvedIndexNames.contains(entry.getKey())).forEach(entry -> {
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
