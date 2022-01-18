/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class FinalizeJobExecutionAction extends ActionType<AcknowledgedResponse> {

    public static final FinalizeJobExecutionAction INSTANCE = new FinalizeJobExecutionAction();
    public static final String NAME = "cluster:internal/xpack/ml/job/finalize_job_execution";

    private FinalizeJobExecutionAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends MasterNodeRequest<Request> {

        private String[] jobIds;

        public Request(String[] jobIds) {
            this.jobIds = jobIds;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            jobIds = in.readStringArray();
        }

        public String[] getJobIds() {
            return jobIds;
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
}
