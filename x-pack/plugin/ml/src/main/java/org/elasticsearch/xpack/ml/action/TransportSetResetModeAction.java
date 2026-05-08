/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.AbstractTransportSetResetModeAction;
import org.elasticsearch.xpack.core.action.SetResetModeActionRequest;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.SetResetModeAction;
import org.elasticsearch.xpack.core.ml.inference.ModelAliasMetadata;

import java.util.function.Consumer;

public class TransportSetResetModeAction extends AbstractTransportSetResetModeAction {

    @Inject
    public TransportSetResetModeAction(
        TransportService transportService,
        ThreadPool threadPool,
        ClusterService clusterService,
        ActionFilters actionFilters,
        ProjectResolver projectResolver
    ) {
        super(SetResetModeAction.NAME, transportService, threadPool, clusterService, actionFilters, projectResolver);
    }

    @Override
    protected boolean isResetMode(ProjectMetadata project) {
        return MlMetadata.getMlMetadata(project).isResetMode();
    }

    @Override
    protected String featureName() {
        return "ml";
    }

    @Override
    protected Consumer<ProjectMetadata.Builder> createProjectUpdate(ProjectMetadata project, SetResetModeActionRequest request) {
        if (request.shouldDeleteMetadata()) {
            assert request.isEnabled() == false; // SetResetModeActionRequest should have enforced this
            return b -> b.removeCustom(MlMetadata.TYPE).removeCustom(ModelAliasMetadata.NAME);
        }
        MlMetadata updatedMetadata = MlMetadata.Builder.from(project.custom(MlMetadata.TYPE)).isResetMode(request.isEnabled()).build();
        return b -> b.putCustom(MlMetadata.TYPE, updatedMetadata);
    }
}
