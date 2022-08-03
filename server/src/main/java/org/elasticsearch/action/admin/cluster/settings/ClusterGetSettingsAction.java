/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.settings;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.Objects;

public class ClusterGetSettingsAction extends ActionType<ClusterGetSettingsAction.Response> {

    public static final ClusterGetSettingsAction INSTANCE = new ClusterGetSettingsAction();
    public static final String NAME = "cluster:monitor/settings";

    public ClusterGetSettingsAction() {
        super(NAME, Response::new);
    }

    /**
     * Request to retrieve the cluster settings
     */
    public static class Request extends MasterNodeReadRequest<Request> {
        public Request() {}

        public Request(StreamInput in) throws IOException {
            super(in);
            assert in.getVersion().onOrAfter(Version.V_8_3_0);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            assert out.getVersion().onOrAfter(Version.V_8_3_0);
            super.writeTo(out);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
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

        public Response(StreamInput in) throws IOException {
            super(in);
            assert in.getVersion().onOrAfter(Version.V_8_3_0);
            persistentSettings = Settings.readSettingsFromStream(in);
            transientSettings = Settings.readSettingsFromStream(in);
            settings = Settings.readSettingsFromStream(in);
        }

        public Response(Settings persistentSettings, Settings transientSettings, Settings settings) {
            this.persistentSettings = Objects.requireNonNullElse(persistentSettings, Settings.EMPTY);
            this.transientSettings = Objects.requireNonNullElse(transientSettings, Settings.EMPTY);
            this.settings = Objects.requireNonNullElse(settings, Settings.EMPTY);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            assert out.getVersion().onOrAfter(Version.V_8_3_0);
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
