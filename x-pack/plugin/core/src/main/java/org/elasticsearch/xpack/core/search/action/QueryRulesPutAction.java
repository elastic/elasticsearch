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
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class QueryRulesPutAction extends ActionType<AcknowledgedResponse> {

    public static final QueryRulesPutAction INSTANCE = new QueryRulesPutAction();
    public static final String NAME = "cluster:admin/read/query_rules/put";

    private QueryRulesPutAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends ActionRequest {

        private final String rulesetId;

        private final BytesReference content;

        private final XContentType contentType;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.rulesetId = in.readString();
            this.content = in.readBytesReference();
            this.contentType = in.readEnum(XContentType.class);
        }

        public Request(String rulesetId, BytesReference content, XContentType contentType) {
            this.rulesetId = rulesetId;
            this.content = content;
            this.contentType = contentType;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            Map<String, Object> sourceMap = XContentHelper.convertToMap(content, false, contentType).v2();
            if (sourceMap.containsKey("mappings") == false) {
                validationException = addValidationError("mappings are missing", validationException);
            } else {
                if (sourceMap.get("mappings") instanceof Map == false) {
                    validationException = addValidationError("mappings definition should be an object", validationException);
                }
            }
            if (sourceMap.containsKey("rules") == false) {
                validationException = addValidationError("rules are missing", validationException);
            } else {
                if (sourceMap.get("rules") instanceof List == false) {
                    validationException = addValidationError("rules definition should be an array", validationException);
                }
            }
            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(rulesetId);
            out.writeBytesReference(content);
            XContentHelper.writeTo(out, contentType);
        }

        public String getRuleSetId() {
            return this.rulesetId;
        }

        public BytesReference getContent() {
            return content;
        }

        public XContentType getContentType() {
            return contentType;
        }
    }

}
