/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.XPackFeatureSet;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 */
public class TransportXPackUsageAction extends HandledTransportAction<XPackUsageRequest, XPackUsageResponse> {

    private final Set<XPackFeatureSet> featureSets;

    @Inject
    public TransportXPackUsageAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                     ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                     Set<XPackFeatureSet> featureSets) {
        super(settings, XPackUsageAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                XPackUsageRequest::new);
        this.featureSets = featureSets;
    }

    @Override
    protected void doExecute(XPackUsageRequest request, ActionListener<XPackUsageResponse> listener) {
        List<XPackFeatureSet.Usage> usages = featureSets.stream().map(XPackFeatureSet::usage).collect(Collectors.toList());
        listener.onResponse(new XPackUsageResponse(usages));
    }
}
