/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.settings;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.local.LocalClusterStateRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class ClusterGetSettingsAction extends ActionType<ClusterGetSettingsAction.Response> {

    public static final ClusterGetSettingsAction INSTANCE = new ClusterGetSettingsAction();
    public static final String NAME = "cluster:monitor/settings";

    public ClusterGetSettingsAction() {
        super(NAME);
    }

    /**
     * Request to retrieve the cluster settings
     */
    public static class Request extends LocalClusterStateRequest {
        public Request(TimeValue masterNodeTimeout) {
            super(masterNodeTimeout);
        }

        /**
         * NB prior to 9.0 this was a TransportMasterNodeReadAction so for BwC we must remain able to read these requests until
         * we no longer need to support calling this action remotely.
         */
        @UpdateForV10(owner = UpdateForV10.Owner.CORE_INFRA)
        public Request(StreamInput in) throws IOException {
            super(in);
            assert in.getTransportVersion().onOrAfter(TransportVersions.V_8_3_0);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers);
        }
    }

    /**
     * Response for cluster settings
     */
    public static class Response extends ActionResponse {
        private final Settings persistentSettings;
        private final Settings transientSettings;
        private final Settings settings;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(persistentSettings, response.persistentSettings)
                && Objects.equals(transientSettings, response.transientSettings)
                && Objects.equals(settings, response.settings);
        }

        @Override
        public int hashCode() {
            return Objects.hash(persistentSettings, transientSettings, settings);
        }

        public Response(Settings persistentSettings, Settings transientSettings, Settings settings) {
            this.persistentSettings = Objects.requireNonNullElse(persistentSettings, Settings.EMPTY);
            this.transientSettings = Objects.requireNonNullElse(transientSettings, Settings.EMPTY);
            this.settings = Objects.requireNonNullElse(settings, Settings.EMPTY);
        }

        /**
         * NB prior to 9.0 get-component was a TransportMasterNodeReadAction so for BwC we must remain able to write these responses until
         * we no longer need to support calling this action remotely.
         */
        @UpdateForV10(owner = UpdateForV10.Owner.CORE_INFRA)
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            assert out.getTransportVersion().onOrAfter(TransportVersions.V_8_3_0);
            persistentSettings.writeTo(out);
            transientSettings.writeTo(out);
            settings.writeTo(out);
        }

        public Settings persistentSettings() {
            return persistentSettings;
        }

        public Settings transientSettings() {
            return transientSettings;
        }

        public Settings settings() {
            return settings;
        }
    }
}
