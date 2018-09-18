/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ccr.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.action.Action;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ccr.AutoFollowStats;

import java.io.IOException;

public class AutoFollowStatsAction extends Action<AutoFollowStatsAction.Response> {

    public static final String NAME = "cluster:monitor/ccr/auto_follow_stats";
    public static final AutoFollowStatsAction INSTANCE = new AutoFollowStatsAction();

    private AutoFollowStatsAction() {
        super(NAME);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends MasterNodeRequest<Request> {

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }


        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private AutoFollowStats stats;

        public Response(AutoFollowStats stats) {
            this.stats = stats;
        }

        public Response() {
        }

        public AutoFollowStats getStats() {
            return stats;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            stats = new AutoFollowStats(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            stats.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            stats.toXContent(builder, params);
            return builder;
        }
    }

}
