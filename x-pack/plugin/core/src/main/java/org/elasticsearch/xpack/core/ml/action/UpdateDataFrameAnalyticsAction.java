/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfigUpdate;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;

import java.io.IOException;
import java.util.Objects;

public class UpdateDataFrameAnalyticsAction extends ActionType<PutDataFrameAnalyticsAction.Response> {

    public static final UpdateDataFrameAnalyticsAction INSTANCE = new UpdateDataFrameAnalyticsAction();
    public static final String NAME = "cluster:admin/xpack/ml/data_frame/analytics/update";

    private UpdateDataFrameAnalyticsAction() {
        super(NAME, PutDataFrameAnalyticsAction.Response::new);
    }

    public static class Request extends AcknowledgedRequest<Request> implements ToXContentObject {

        /**
         * Parses request.
         */
        public static Request parseRequest(String id, XContentParser parser) {
            DataFrameAnalyticsConfigUpdate.Builder updateBuilder = DataFrameAnalyticsConfigUpdate.PARSER.apply(parser, null);
            if (updateBuilder.getId() == null) {
                updateBuilder.setId(id);
            } else if (Strings.isNullOrEmpty(id) == false && id.equals(updateBuilder.getId()) == false) {
                // If we have both URI and body ID, they must be identical
                throw new IllegalArgumentException(
                    Messages.getMessage(Messages.INCONSISTENT_ID, DataFrameAnalyticsConfig.ID, updateBuilder.getId(), id)
                );
            }

            return new UpdateDataFrameAnalyticsAction.Request(updateBuilder.build());
        }

        private DataFrameAnalyticsConfigUpdate update;

        public Request(StreamInput in) throws IOException {
            super(in);
            update = new DataFrameAnalyticsConfigUpdate(in);
        }

        public Request(DataFrameAnalyticsConfigUpdate update) {
            this.update = update;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            update.writeTo(out);
        }

        public DataFrameAnalyticsConfigUpdate getUpdate() {
            return update;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            update.toXContent(builder, params);
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            UpdateDataFrameAnalyticsAction.Request request = (UpdateDataFrameAnalyticsAction.Request) o;
            return Objects.equals(update, request.update);
        }

        @Override
        public int hashCode() {
            return Objects.hash(update);
        }
    }
}
