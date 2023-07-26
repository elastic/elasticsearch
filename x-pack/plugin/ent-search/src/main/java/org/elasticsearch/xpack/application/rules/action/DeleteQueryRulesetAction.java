/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class DeleteQueryRulesetAction extends ActionType<AcknowledgedResponse> {

    public static final DeleteQueryRulesetAction INSTANCE = new DeleteQueryRulesetAction();
    public static final String NAME = "cluster:admin/xpack/query_rules/delete";

    private DeleteQueryRulesetAction() {
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
            ActionRequestValidationException validationException = null;

            if (Strings.isNullOrEmpty(rulesetId)) {
                validationException = addValidationError("ruleset_id missing", validationException);
            }

            return validationException;
        }

        public String rulesetId() {
            return rulesetId;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(rulesetId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request that = (Request) o;
            return Objects.equals(rulesetId, that.rulesetId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(rulesetId);
        }
    }

}
