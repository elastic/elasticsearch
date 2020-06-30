/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.XPackLicenseState.Feature;
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

    private final XPackLicenseState licenseState;

    @Inject
    public VotingOnlyNodeFeatureSet(@Nullable XPackLicenseState licenseState) {
        this.licenseState = licenseState;
    }

    @Override
    public String name() {
        return XPackField.VOTING_ONLY;
    }

    @Override
    public boolean available() {
        return licenseState != null && licenseState.isAllowed(Feature.VOTING_ONLY);
    }

    @Override
    public boolean enabled() {
        return true;
    }

    public static class UsageTransportAction extends XPackUsageFeatureTransportAction {

        private final XPackLicenseState licenseState;

        @Inject
        public UsageTransportAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                    ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                    XPackLicenseState licenseState) {
            super(XPackUsageFeatureAction.VOTING_ONLY.name(), transportService, clusterService,
                threadPool, actionFilters, indexNameExpressionResolver);
            this.licenseState = licenseState;
        }

        @Override
        protected void masterOperation(Task task, XPackUsageRequest request, ClusterState state,
                                       ActionListener<XPackUsageFeatureResponse> listener) {
            final boolean available = licenseState.isAllowed(Feature.VOTING_ONLY);
            final VotingOnlyNodeFeatureSetUsage usage =
                new VotingOnlyNodeFeatureSetUsage(available);
            listener.onResponse(new XPackUsageFeatureResponse(usage));
        }
    }

    public static class UsageInfoAction extends XPackInfoFeatureTransportAction {

        private final XPackLicenseState licenseState;

        @Inject
        public UsageInfoAction(TransportService transportService, ActionFilters actionFilters,
                               XPackLicenseState licenseState) {
            super(XPackInfoFeatureAction.VOTING_ONLY.name(), transportService, actionFilters);
            this.licenseState = licenseState;
        }

        @Override
        protected String name() {
            return XPackField.VOTING_ONLY;
        }

        @Override
        protected boolean available() {
            return licenseState.isAllowed(Feature.VOTING_ONLY);
        }

        @Override
        protected boolean enabled() {
            return true;
        }
    }
}
