/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

public class FinalizeJobExecutionAction extends Action<AcknowledgedResponse> {

    public static final FinalizeJobExecutionAction INSTANCE = new FinalizeJobExecutionAction();
    public static final String NAME = "cluster:internal/xpack/ml/job/finalize_job_execution";

    private FinalizeJobExecutionAction() {
        super(NAME);
    }

    @Override
    public AcknowledgedResponse newResponse() {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    public Writeable.Reader<AcknowledgedResponse> getResponseReader() {
        return AcknowledgedResponse::new;
    }

    public static class Request extends MasterNodeRequest<Request> {

        private String[] jobIds;

        public Request(String[] jobIds) {
            this.jobIds = jobIds;
        }

        public Request() {
        }

        public String[] getJobIds() {
            return jobIds;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            jobIds = in.readStringArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(jobIds);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class RequestBuilder
            extends MasterNodeOperationRequestBuilder<Request, AcknowledgedResponse, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, FinalizeJobExecutionAction action) {
            super(client, action, new Request());
        }
    }
}
