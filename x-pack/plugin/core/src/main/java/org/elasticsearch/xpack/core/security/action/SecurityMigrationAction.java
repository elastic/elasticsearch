/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action;

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
 * Applies one-time, versioned data migrations to the {@code .security} index. Submitted whenever the next pending
 * migration is ready to run; the transport action runs on the elected master, applies any outstanding migrations in
 * ascending version order, and commits progress to cluster state so that it survives retries.
 */
public class SecurityMigrationAction extends ActionType<ActionResponse.Empty> {

    public static final SecurityMigrationAction INSTANCE = new SecurityMigrationAction();
    public static final String NAME = "internal:security/migration/start";

    private SecurityMigrationAction() {
        super(NAME);
    }

    public static class Request extends MasterNodeRequest<Request> {

        private final ProjectId projectId;
        private final int migrationVersion;
        private final boolean migrationNeeded;

        public Request(TimeValue masterNodeTimeout, ProjectId projectId, int migrationVersion, boolean migrationNeeded) {
            super(masterNodeTimeout);
            this.projectId = projectId;
            this.migrationVersion = migrationVersion;
            this.migrationNeeded = migrationNeeded;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.projectId = ProjectId.readFrom(in);
            this.migrationVersion = in.readInt();
            this.migrationNeeded = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            projectId.writeTo(out);
            out.writeInt(migrationVersion);
            out.writeBoolean(migrationNeeded);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public ProjectId getProjectId() {
            return projectId;
        }

        public int getMigrationVersion() {
            return migrationVersion;
        }

        public boolean isMigrationNeeded() {
            return migrationNeeded;
        }
    }
}
