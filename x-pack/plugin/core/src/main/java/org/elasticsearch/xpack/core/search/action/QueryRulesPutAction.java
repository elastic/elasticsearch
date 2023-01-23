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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class QueryRulesPutAction extends ActionType<AcknowledgedResponse> {

    public static final QueryRulesPutAction INSTANCE = new QueryRulesPutAction();
    public static final String NAME = "cluster:admin/read/query_rules/put";

    private QueryRulesPutAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends ActionRequest {

        private final String rulesetIds;
        public final BytesReference content;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.rulesetIds = in.readString();
            this.content = in.readBytesReference();
        }

        public Request(String rulesetId, BytesReference content) {
            this.rulesetIds = rulesetId;
            this.content = content;
        }

        @Override
        public ActionRequestValidationException validate() {
            // TODO check id and body non null, body has required fields etc...
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(rulesetIds);
            out.writeBytesReference(content);
        }

        public String getRuleSetId() {
            return this.rulesetIds;
        }
    }

}
