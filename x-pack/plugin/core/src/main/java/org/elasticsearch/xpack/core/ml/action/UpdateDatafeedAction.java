/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedUpdate;
import org.elasticsearch.xpack.core.security.cloud.CloudCredential;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig.DATAFEED_CLOUD_INTERNAL_CREDENTIAL;

public class UpdateDatafeedAction extends ActionType<PutDatafeedAction.Response> {

    public static final UpdateDatafeedAction INSTANCE = new UpdateDatafeedAction();
    public static final String NAME = "cluster:admin/xpack/ml/datafeeds/update";

    private UpdateDatafeedAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> implements ToXContentObject, Releasable {

        public static Request parseRequest(String datafeedId, @Nullable IndicesOptions indicesOptions, XContentParser parser) {
            DatafeedUpdate.Builder update = DatafeedUpdate.PARSER.apply(parser, null);
            if (indicesOptions != null) {
                update.setIndicesOptions(indicesOptions);
            }
            update.setId(datafeedId);
            return new Request(update.build());
        }

        private DatafeedUpdate update;

        @Nullable
        private CloudCredential cloudCredential;

        public Request(DatafeedUpdate update) {
            super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT, DEFAULT_ACK_TIMEOUT);
            this.update = update;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            update = new DatafeedUpdate(in);
            if (in.getTransportVersion().supports(DATAFEED_CLOUD_INTERNAL_CREDENTIAL)) {
                cloudCredential = in.readOptionalWriteable(CloudCredential::new);
            } else {
                cloudCredential = null;
            }
        }

        public DatafeedUpdate getUpdate() {
            return update;
        }

        @Nullable
        public CloudCredential getCloudCredential() {
            return cloudCredential;
        }

        public void setCloudCredential(@Nullable CloudCredential cloudCredential) {
            this.cloudCredential = cloudCredential;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            update.writeTo(out);
            if (out.getTransportVersion().supports(DATAFEED_CLOUD_INTERNAL_CREDENTIAL)) {
                out.writeOptionalWriteable(cloudCredential);
            }
        }

        @Override
        public void close() {
            IOUtils.closeWhileHandlingException(cloudCredential);
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
            // cloudCredential is intentionally excluded: request-scoped secret carrier, not logical identity.
            return Objects.equals(update, request.update);
        }

        @Override
        public int hashCode() {
            return Objects.hash(update);
        }
    }
}
