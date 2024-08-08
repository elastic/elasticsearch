/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.cluster.coordination.votingonly;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureTransportAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.votingonly.VotingOnlyNodeFeatureSetUsage;

public class VotingOnlyNodeFeatureSet implements XPackFeatureSet {

    @Override
    public String name() {
        return XPackField.VOTING_ONLY;
    }

    @Override
    public boolean available() {
        return true;
    }

    @Override
    public boolean enabled() {
        return true;
    }

    public static class UsageTransportAction extends XPackUsageFeatureTransportAction {

        @Inject
        public UsageTransportAction(
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver
        ) {
            super(
                XPackUsageFeatureAction.VOTING_ONLY.name(),
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                indexNameExpressionResolver
            );
        }

        @Override
        protected void masterOperation(
            Task task,
            XPackUsageRequest request,
            ClusterState state,
            ActionListener<XPackUsageFeatureResponse> listener
        ) {
            final VotingOnlyNodeFeatureSetUsage usage = new VotingOnlyNodeFeatureSetUsage();
            listener.onResponse(new XPackUsageFeatureResponse(usage));
        }
    }

    public static class UsageInfoAction extends XPackInfoFeatureTransportAction {

        @Inject
        public UsageInfoAction(TransportService transportService, ActionFilters actionFilters) {
            super(XPackInfoFeatureAction.VOTING_ONLY.name(), transportService, actionFilters);
        }

        @Override
        protected String name() {
            return XPackField.VOTING_ONLY;
        }

        @Override
        protected boolean available() {
            return true;
        }

        @Override
        protected boolean enabled() {
            return true;
        }
    }
}
