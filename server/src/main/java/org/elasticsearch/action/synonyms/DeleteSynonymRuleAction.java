/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.synonyms;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

public class DeleteSynonymRuleAction extends ActionType<SynonymUpdateResponse> {

    public static final DeleteSynonymRuleAction INSTANCE = new DeleteSynonymRuleAction();
    public static final String NAME = "cluster:admin/synonym_rules/delete";

    public DeleteSynonymRuleAction() {
        super(NAME);
    }

    public static class Request extends ActionRequest {
        private final String synonymsSetId;
        private final String synonymRuleId;
        private final boolean refresh;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.synonymsSetId = in.readString();
            this.synonymRuleId = in.readString();
            if (in.getTransportVersion().onOrAfter(TransportVersions.SYNONYMS_REFRESH_PARAM)) {
                this.refresh = in.readBoolean();
            } else {
                this.refresh = true;
            }
        }

        public Request(String synonymsSetId, String synonymRuleId, boolean refresh) {
            this.synonymsSetId = synonymsSetId;
            this.synonymRuleId = synonymRuleId;
            this.refresh = refresh;
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
            if (out.getTransportVersion().onOrAfter(TransportVersions.SYNONYMS_REFRESH_PARAM)) {
                out.writeBoolean(refresh);
            }
        }

        public String synonymsSetId() {
            return synonymsSetId;
        }

        public String synonymRuleId() {
            return synonymRuleId;
        }

        public boolean refresh() {
            return refresh;
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
}
