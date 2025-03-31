/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reservedstate.service;

import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collection;

class ProjectClusterStateHandlerAdapter<T> implements ReservedClusterStateHandler<ClusterState, T> {

    private final ProjectId projectId;
    private final ReservedClusterStateHandler<ProjectMetadata, T> handler;

    ProjectClusterStateHandlerAdapter(ProjectId projectId, ReservedClusterStateHandler<ProjectMetadata, T> handler) {
        this.projectId = projectId;
        this.handler = handler;
    }

    @Override
    public String name() {
        return handler.name();
    }

    @Override
    public Collection<String> dependencies() {
        return handler.dependencies();
    }

    @Override
    public Collection<String> optionalDependencies() {
        return handler.optionalDependencies();
    }

    @Override
    public void validate(MasterNodeRequest<?> request) {
        handler.validate(request);
    }

    @Override
    public T fromXContent(XContentParser parser) throws IOException {
        return handler.fromXContent(parser);
    }

    @Override
    public TransformState<ClusterState> transform(T source, TransformState<ClusterState> prevState) throws Exception {
        ProjectMetadata project = prevState.state().metadata().getProject(projectId);

        TransformState<ProjectMetadata> oldProjectState = new TransformState<>(project, prevState.keys());
        TransformState<ProjectMetadata> newProjectState = handler.transform(source, oldProjectState);

        return newProjectState == oldProjectState
            ? prevState
            : new TransformState<>(
                ClusterState.builder(prevState.state()).putProjectMetadata(newProjectState.state()).build(),
                newProjectState.keys()
            );
    }

    @Override
    public String toString() {
        return "ProjectClusterStateHandlerAdapter[" + handler.toString() + "]";
    }
}
