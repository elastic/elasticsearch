/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xpack.core.action.AbstractGetResourcesResponse;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class GetDatafeedsAction extends ActionType<GetDatafeedsAction.Response> {

    public static final GetDatafeedsAction INSTANCE = new GetDatafeedsAction();
    public static final String NAME = "cluster:monitor/xpack/ml/datafeeds/get";

    public static final String ALL = "_all";

    private GetDatafeedsAction() {
        super(NAME, Response::new);
    }

    public static class Request extends MasterNodeReadRequest<Request> {

        public static final String ALLOW_NO_MATCH = "allow_no_match";

        private String datafeedId;
        private boolean allowNoMatch = true;

        public Request(String datafeedId) {
            this();
            this.datafeedId = ExceptionsHelper.requireNonNull(datafeedId, DatafeedConfig.ID.getPreferredName());
        }

        public Request() {
            local(true);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            datafeedId = in.readString();
            allowNoMatch = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(datafeedId);
            out.writeBoolean(allowNoMatch);
        }

        public String getDatafeedId() {
            return datafeedId;
        }

        public boolean allowNoMatch() {
            return allowNoMatch;
        }

        public void setAllowNoMatch(boolean allowNoMatch) {
            this.allowNoMatch = allowNoMatch;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public int hashCode() {
            return Objects.hash(datafeedId, allowNoMatch);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(datafeedId, other.datafeedId) && Objects.equals(allowNoMatch, other.allowNoMatch);
        }
    }

    public static class Response extends AbstractGetResourcesResponse<DatafeedConfig> implements ToXContentObject {

        public Response(QueryPage<DatafeedConfig> datafeeds) {
            super(datafeeds);
        }

        public Response(StreamInput in) throws IOException {
            super(in);
        }

        public QueryPage<DatafeedConfig> getResponse() {
            return getResources();
        }

        protected Reader<DatafeedConfig> getReader() {
            return DatafeedConfig::new;
        }
    }

}
