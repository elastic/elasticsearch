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
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class GetWatcherSettingsAction extends ActionType<GetWatcherSettingsAction.Response> {

    public static final GetWatcherSettingsAction INSTANCE = new GetWatcherSettingsAction();
    public static final String NAME = "cluster:admin/xpack/watcher/settings/get";

    public GetWatcherSettingsAction() {
        super(NAME);
    }

    public static class Request extends MasterNodeReadRequest<Request> {

        public Request(TimeValue masterNodeTimeout) {
            super(masterNodeTimeout);
        }

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
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
                super.writeTo(out);
            }
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final Settings settings;

        public Response(Settings settings) {
            this.settings = settings;
        }

        public Response(StreamInput in) throws IOException {
            this.settings = Settings.readSettingsFromStream(in);
        }

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
