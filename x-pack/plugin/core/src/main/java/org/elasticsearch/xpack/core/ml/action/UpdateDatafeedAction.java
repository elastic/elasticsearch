/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedUpdate;

import java.io.IOException;
import java.util.Objects;

public class UpdateDatafeedAction extends ActionType<PutDatafeedAction.Response> {

    public static final UpdateDatafeedAction INSTANCE = new UpdateDatafeedAction();
    public static final String NAME = "cluster:admin/xpack/ml/datafeeds/update";

    private UpdateDatafeedAction() {
        super(NAME, PutDatafeedAction.Response::new);
    }

    public static class Request extends AcknowledgedRequest<Request> implements ToXContentObject {

        public static Request parseRequest(String datafeedId, @Nullable IndicesOptions indicesOptions, XContentParser parser) {
            DatafeedUpdate.Builder update = DatafeedUpdate.PARSER.apply(parser, null);
            if (indicesOptions != null) {
                update.setIndicesOptions(indicesOptions);
            }
            update.setId(datafeedId);
            return new Request(update.build());
        }

        private DatafeedUpdate update;

        public Request(DatafeedUpdate update) {
            this.update = update;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            update = new DatafeedUpdate(in);
        }

        public DatafeedUpdate getUpdate() {
            return update;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            update.writeTo(out);
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
            Request request = (Request) o;
            return Objects.equals(update, request.update);
        }

        @Override
        public int hashCode() {
            return Objects.hash(update);
        }
    }
}
