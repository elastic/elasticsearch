/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.synonyms;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.admin.indices.analyze.ReloadAnalyzersResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.support.master.AcknowledgedResponse.ACKNOWLEDGED_KEY;

public class DeleteSynonymRuleAction extends ActionType<DeleteSynonymRuleAction.Response> {

    public static final DeleteSynonymRuleAction INSTANCE = new DeleteSynonymRuleAction();
    public static final String NAME = "cluster:admin/synonym_rules/delete";

    public DeleteSynonymRuleAction() {
        super(NAME, DeleteSynonymRuleAction.Response::new);
    }

    public static class Request extends ActionRequest {
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

    // TODO Remove duplicates
    public static class Response extends ActionResponse implements ToXContentObject {

        private final AcknowledgedResponse acknowledgedResponse;
        private final ReloadAnalyzersResponse reloadAnalyzersResponse;

        public Response(StreamInput in) throws IOException {
            super(in);
            this.acknowledgedResponse = AcknowledgedResponse.readFrom(in);
            this.reloadAnalyzersResponse = new ReloadAnalyzersResponse(in);
        }

        public Response(AcknowledgedResponse acknowledgedResponse, ReloadAnalyzersResponse reloadAnalyzersResponse) {
            super();
            Objects.requireNonNull(acknowledgedResponse, "Acknowledge response must not be null");
            Objects.requireNonNull(reloadAnalyzersResponse, "Reload analyzers response must not be null");
            this.acknowledgedResponse = acknowledgedResponse;
            this.reloadAnalyzersResponse = reloadAnalyzersResponse;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            {
                builder.field(ACKNOWLEDGED_KEY, acknowledgedResponse.isAcknowledged());
                builder.field("reload_analyzers_details");
                reloadAnalyzersResponse.toXContent(builder, params);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            acknowledgedResponse.writeTo(out);
            reloadAnalyzersResponse.writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DeleteSynonymRuleAction.Response response = (DeleteSynonymRuleAction.Response) o;
            return Objects.equals(acknowledgedResponse, response.acknowledgedResponse)
                && Objects.equals(reloadAnalyzersResponse, response.reloadAnalyzersResponse);
        }

        @Override
        public int hashCode() {
            return Objects.hash(acknowledgedResponse, reloadAnalyzersResponse);
        }
    }
}
