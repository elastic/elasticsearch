/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.synonyms;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.synonyms.SynonymRule;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class GetSynonymRuleAction extends ActionType<GetSynonymRuleAction.Response> {

    public static final GetSynonymRuleAction INSTANCE = new GetSynonymRuleAction();
    public static final String NAME = "cluster:admin/synonym_rules/get";

    public GetSynonymRuleAction() {
        super(NAME);
    }

    public static class Request extends LegacyActionRequest {
        private final String synonymsSetId;

        private final String synonymRuleId;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.synonymsSetId = in.readString();
            this.synonymRuleId = in.readString();
        }

        public Request(String synonymsSetId, String synonymRuleId) {
            this.synonymsSetId = synonymsSetId;
            this.synonymRuleId = synonymRuleId;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (Strings.isNullOrEmpty(synonymsSetId)) {
                validationException = ValidateActions.addValidationError("synonyms set must be specified", validationException);
            }
            if (Strings.isNullOrEmpty(synonymRuleId)) {
                validationException = ValidateActions.addValidationError("synonym rule id must be specified", validationException);
            }

            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(synonymsSetId);
            out.writeString(synonymRuleId);
        }

        public String synonymsSetId() {
            return synonymsSetId;
        }

        public String synonymRuleId() {
            return synonymRuleId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(synonymsSetId, request.synonymsSetId) && Objects.equals(synonymRuleId, request.synonymRuleId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(synonymsSetId, synonymRuleId);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final SynonymRule synonymRule;

        public Response(StreamInput in) throws IOException {
            super(in);
            this.synonymRule = new SynonymRule(in);
        }

        public Response(SynonymRule synonymRule) {
            this.synonymRule = synonymRule;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            synonymRule.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            synonymRule.toXContent(builder, params);
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(synonymRule, response.synonymRule);
        }

        @Override
        public int hashCode() {
            return Objects.hash(synonymRule);
        }
    }
}
