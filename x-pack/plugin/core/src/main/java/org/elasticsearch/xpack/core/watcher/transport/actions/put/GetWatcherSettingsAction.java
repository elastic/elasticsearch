/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.watcher.transport.actions.put;

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
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

public class GetWatcherSettingsAction extends ActionType<GetWatcherSettingsAction.Response> {

    public static final GetWatcherSettingsAction INSTANCE = new GetWatcherSettingsAction();
    public static final String NAME = "cluster:admin/xpack/watcher/settings/get";

    public GetWatcherSettingsAction() {
        super(NAME);
    }

    public static class Request extends LocalClusterStateRequest {

        public Request(TimeValue masterNodeTimeout) {
            super(masterNodeTimeout);
        }

        /**
         * NB prior to 9.0 this was a TransportMasterNodeReadAction so for BwC we must remain able to read these requests until
         * we no longer need to support calling this action remotely.
         */
        @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
        public static Request readFrom(StreamInput in) throws IOException {
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
                return new Request(in);
            } else {
                return new Request(TimeValue.THIRTY_SECONDS);
            }
        }

        private Request(StreamInput in) throws IOException {
            super(in);
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

    public static class Response extends ActionResponse implements ToXContentObject {

        private final Settings settings;

        public Response(Settings settings) {
            this.settings = settings;
        }

        /**
         * NB prior to 9.0 this was a TransportMasterNodeReadAction so for BwC we must remain able to write these responses until
         * we no longer need to support calling this action remotely.
         */
        @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            this.settings.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            this.settings.toXContent(builder, params);
            builder.endObject();
            return builder;
        }
    }
}
