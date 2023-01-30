/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.search.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class QueryRulesDeleteAction extends ActionType<AcknowledgedResponse> {

    public static final QueryRulesDeleteAction INSTANCE = new QueryRulesDeleteAction();
    public static final String NAME = "cluster:admin/read/query_rules/delete";

    private QueryRulesDeleteAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends ActionRequest {

        private final String rulesetId;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.rulesetId = in.readString();
        }

        public Request(String rulesetId) {
            this.rulesetId = rulesetId;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(rulesetId);
        }

        public String getRuleSetId() {
            return this.rulesetId;
        }

    }
}
