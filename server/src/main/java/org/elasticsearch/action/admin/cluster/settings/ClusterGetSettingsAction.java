/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.settings;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.admin.cluster.settings.RestClusterGetSettingsResponse.PERSISTENT_FIELD;
import static org.elasticsearch.action.admin.cluster.settings.RestClusterGetSettingsResponse.TRANSIENT_FIELD;

public class ClusterGetSettingsAction extends ActionType<ClusterGetSettingsAction.Response> {

    public static final ClusterGetSettingsAction INSTANCE = new ClusterGetSettingsAction();
    public static final String NAME = "cluster:admin/settings/get";

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
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    /**
     * Cluster get settings request builder
     */
    public static class RequestBuilder extends MasterNodeReadOperationRequestBuilder<Request, Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, ClusterGetSettingsAction action) {
            super(client, action, new Request());
        }
    }

    /**
     * Response for cluster settings
     */
    public static class Response extends ActionResponse implements ToXContentObject {
        private final Settings persistentSettings;
        private final Settings transientSettings;

        public Response(StreamInput in) throws IOException {
            super(in);
            persistentSettings = Settings.readSettingsFromStream(in);
            transientSettings = Settings.readSettingsFromStream(in);
        }

        public Response(Settings persistentSettings, Settings transientSettings) {
            this.persistentSettings = Objects.requireNonNullElse(persistentSettings, Settings.EMPTY);
            this.transientSettings = Objects.requireNonNullElse(transientSettings, Settings.EMPTY);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();

            builder.startObject(PERSISTENT_FIELD);
            persistentSettings.toXContent(builder, params);
            builder.endObject();

            builder.startObject(TRANSIENT_FIELD);
            transientSettings.toXContent(builder, params);
            builder.endObject();

            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            Settings.writeSettingsToStream(persistentSettings, out);
            Settings.writeSettingsToStream(transientSettings, out);
        }

        public Settings persistentSettings() {
            return persistentSettings;
        }

        public Settings transientSettings() {
            return transientSettings;
        }

        public Settings settings() {
            return Settings.builder().put(persistentSettings).put(transientSettings).build();
        }
    }
}
