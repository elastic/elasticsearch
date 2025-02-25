/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.AbstractTransportSetResetModeAction;
import org.elasticsearch.xpack.core.action.SetResetModeActionRequest;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.SetResetModeAction;
import org.elasticsearch.xpack.core.ml.inference.ModelAliasMetadata;

public class TransportSetResetModeAction extends AbstractTransportSetResetModeAction {

    @Inject
    public TransportSetResetModeAction(
        TransportService transportService,
        ThreadPool threadPool,
        ClusterService clusterService,
        ActionFilters actionFilters
    ) {
        super(SetResetModeAction.NAME, transportService, threadPool, clusterService, actionFilters);
    }

    @Override
    protected boolean isResetMode(ClusterState clusterState) {
        return MlMetadata.getMlMetadata(clusterState).isResetMode();
    }

    @Override
    protected String featureName() {
        return "ml";
    }

    @Override
    protected ClusterState setState(ClusterState oldState, SetResetModeActionRequest request) {
        final ProjectMetadata project = oldState.metadata().getDefaultProject();
        final ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(project);
        if (request.shouldDeleteMetadata()) {
            assert request.isEnabled() == false; // SetResetModeActionRequest should have enforced this
            projectBuilder.removeCustom(MlMetadata.TYPE).removeCustom(ModelAliasMetadata.NAME);
        } else {
            MlMetadata.Builder builder = MlMetadata.Builder.from(project.custom(MlMetadata.TYPE)).isResetMode(request.isEnabled());
            projectBuilder.putCustom(MlMetadata.TYPE, builder.build());
        }
        return ClusterState.builder(oldState).putProjectMetadata(projectBuilder.build()).build();
    }
}
