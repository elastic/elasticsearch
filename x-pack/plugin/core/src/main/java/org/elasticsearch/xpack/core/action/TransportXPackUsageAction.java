/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackFeatureSet.Usage;
import org.elasticsearch.xpack.core.common.IteratingActionListener;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiConsumer;

public class TransportXPackUsageAction extends TransportMasterNodeAction<XPackUsageRequest, XPackUsageResponse> {

    private final List<XPackFeatureSet> featureSets;

    @Inject
    public TransportXPackUsageAction(ThreadPool threadPool, TransportService transportService,
                                     ClusterService clusterService, ActionFilters actionFilters,
                                     IndexNameExpressionResolver indexNameExpressionResolver, Set<XPackFeatureSet> featureSets) {
        super(XPackUsageAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
                XPackUsageRequest::new);
        this.featureSets = Collections.unmodifiableList(new ArrayList<>(featureSets));
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected XPackUsageResponse newResponse() {
        return new XPackUsageResponse();
    }

    @Override
    protected void masterOperation(XPackUsageRequest request, ClusterState state, ActionListener<XPackUsageResponse> listener) {
        final ActionListener<List<XPackFeatureSet.Usage>> usageActionListener = new ActionListener<List<Usage>>() {
            @Override
            public void onResponse(List<Usage> usages) {
                listener.onResponse(new XPackUsageResponse(usages));
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        };
        final AtomicReferenceArray<Usage> featureSetUsages = new AtomicReferenceArray<>(featureSets.size());
        final AtomicInteger position = new AtomicInteger(0);
        final BiConsumer<XPackFeatureSet, ActionListener<List<Usage>>> consumer = (featureSet, iteratingListener) -> {
            featureSet.usage(new ActionListener<Usage>() {
                @Override
                public void onResponse(Usage usage) {
                    featureSetUsages.set(position.getAndIncrement(), usage);
                    // the value sent back doesn't matter since our predicate keeps iterating
                    iteratingListener.onResponse(Collections.emptyList());
                }

                @Override
                public void onFailure(Exception e) {
                    iteratingListener.onFailure(e);
                }
            });
        };
        IteratingActionListener<List<XPackFeatureSet.Usage>, XPackFeatureSet> iteratingActionListener =
                new IteratingActionListener<>(usageActionListener, consumer, featureSets,
                        threadPool.getThreadContext(), (ignore) -> {
                    final List<Usage> usageList = new ArrayList<>(featureSetUsages.length());
                    for (int i = 0; i < featureSetUsages.length(); i++) {
                        usageList.add(featureSetUsages.get(i));
                    }
                    return usageList;
                }, (ignore) -> true);
        iteratingActionListener.run();
    }

    @Override
    protected ClusterBlockException checkBlock(XPackUsageRequest request, ClusterState state) {
        return null;
    }
}
