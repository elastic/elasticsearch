/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.system_indices.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;

/**
 * Runs the one-shot system index migration on the elected master. Submitted by
 * {@link TransportPostFeatureUpgradeAction} when one or more system indices require migration;
 * the transport action executes {@link org.elasticsearch.system_indices.task.SystemIndexMigrator}
 * directly on the master node, storing progress in cluster state via
 * {@link org.elasticsearch.system_indices.task.FeatureMigrationResults}.
 */
public class SystemIndexMigrationAction extends ActionType<ActionResponse.Empty> {

    public static final SystemIndexMigrationAction INSTANCE = new SystemIndexMigrationAction();
    public static final String NAME = "internal:cluster/migration/system-indices/start";

    private SystemIndexMigrationAction() {
        super(NAME);
    }

    public static class Request extends MasterNodeRequest<Request> {

        private final ProjectId projectId;

        public Request(TimeValue masterNodeTimeout, ProjectId projectId) {
            super(masterNodeTimeout);
            this.projectId = projectId;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.projectId = ProjectId.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            projectId.writeTo(out);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public ProjectId getProjectId() {
            return projectId;
        }
    }
}
