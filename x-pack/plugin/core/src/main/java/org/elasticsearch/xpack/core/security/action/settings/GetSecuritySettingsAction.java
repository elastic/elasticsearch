/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.settings;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.xpack.core.security.action.settings.UpdateSecuritySettingsAction.MAIN_INDEX_NAME;
import static org.elasticsearch.xpack.core.security.action.settings.UpdateSecuritySettingsAction.PROFILES_INDEX_NAME;
import static org.elasticsearch.xpack.core.security.action.settings.UpdateSecuritySettingsAction.TOKENS_INDEX_NAME;

public class GetSecuritySettingsAction extends ActionType<GetSecuritySettingsAction.Response> {

    public static final GetSecuritySettingsAction INSTANCE = new GetSecuritySettingsAction();
    public static final String NAME = "cluster:admin/xpack/security/settings/get";

    public GetSecuritySettingsAction() {
        super(NAME, GetSecuritySettingsAction.Response::new);
    }

    public static class Request extends MasterNodeReadRequest<GetSecuritySettingsAction.Request> {

        public Request() {}

        public Request(StreamInput in) throws IOException {}

        @Override
        public void writeTo(StreamOutput out) throws IOException {}

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {
        private final Settings mainIndexSettings;
        private final Settings tokensIndexSettings;
        private final Settings profilesIndexSettings;

        public Response(Settings mainIndexSettings, Settings tokensIndexSettings, Settings profilesIndexSettings) {
            this.mainIndexSettings = mainIndexSettings;
            this.tokensIndexSettings = tokensIndexSettings;
            this.profilesIndexSettings = profilesIndexSettings;
        }

        public Response(StreamInput in) throws IOException {
            this.mainIndexSettings = Settings.readSettingsFromStream(in);
            this.tokensIndexSettings = Settings.readSettingsFromStream(in);
            this.profilesIndexSettings = Settings.readSettingsFromStream(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            this.mainIndexSettings.writeTo(out);
            this.tokensIndexSettings.writeTo(out);
            this.profilesIndexSettings.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startObject(MAIN_INDEX_NAME);
            {
                this.mainIndexSettings.toXContent(builder, params);
            }
            builder.endObject();
            builder.startObject(TOKENS_INDEX_NAME);
            {
                this.tokensIndexSettings.toXContent(builder, params);
            }
            builder.endObject();
            builder.startObject(PROFILES_INDEX_NAME);
            {
                this.profilesIndexSettings.toXContent(builder, params);
            }
            builder.endObject();
            builder.endObject();
            return builder;
        }
    }
}
