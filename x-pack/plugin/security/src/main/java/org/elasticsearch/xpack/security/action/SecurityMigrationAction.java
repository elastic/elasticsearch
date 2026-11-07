/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.UntypedActionRequest;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;

/// Node-local action that applies the outstanding migrations to the `.security` index. Migrations are one-shot and retried by
/// [org.elasticsearch.xpack.security.support.SecurityMigrations.Manager], so they run as a regular task rather than a persistent task.
public class SecurityMigrationAction extends ActionType<ActionResponse.Empty> {

    public static final String NAME = "internal:index/metadata/security_migration/run";
    public static final SecurityMigrationAction INSTANCE = new SecurityMigrationAction();

    private SecurityMigrationAction() {
        super(NAME);
    }

    public static class Request extends UntypedActionRequest {

        private final int migrationVersion;
        private final boolean migrationNeeded;

        public Request(int migrationVersion, boolean migrationNeeded) {
            this.migrationVersion = migrationVersion;
            this.migrationNeeded = migrationNeeded;
        }

        public int getMigrationVersion() {
            return migrationVersion;
        }

        public boolean isMigrationNeeded() {
            return migrationNeeded;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
        }

        @Override
        public String getDescription() {
            return "security migration from version [" + migrationVersion + "], migration needed [" + migrationNeeded + "]";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }
    }
}
