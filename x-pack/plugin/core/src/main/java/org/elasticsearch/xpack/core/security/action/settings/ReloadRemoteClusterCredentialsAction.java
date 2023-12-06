/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.settings;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;

public class ReloadRemoteClusterCredentialsAction extends ActionType<ActionResponse.Empty> {
    public static final String NAME = "cluster:admin/xpack/security/reload_remote_cluster_credentials";
    public static final ReloadRemoteClusterCredentialsAction INSTANCE = new ReloadRemoteClusterCredentialsAction();

    private ReloadRemoteClusterCredentialsAction() {
        super(NAME, Writeable.Reader.localOnly());
    }

    public static class Request extends ActionRequest {
        private final Settings settings;

        public Request(Settings settings) {
            this.settings = settings;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public Settings getSettings() {
            return settings;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }
    }
}
