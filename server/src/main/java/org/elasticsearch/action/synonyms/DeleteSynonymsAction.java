/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.synonyms;

import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.analyze.ReloadAnalyzersResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.support.master.AcknowledgedResponse.ACKNOWLEDGED_KEY;

public class DeleteSynonymsAction extends ActionType<DeleteSynonymsAction.Response> {

    public static final DeleteSynonymsAction INSTANCE = new DeleteSynonymsAction();
    public static final String NAME = "cluster:admin/synonyms/delete";

    public DeleteSynonymsAction() {
        super(NAME, Response::new);
    }

    public static class Request extends ActionRequest {
        private final String synonymsSetId;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.synonymsSetId = in.readString();
        }

        public Request(String synonymsSetId) {
            if (Strings.isBlank(synonymsSetId)) {
                throw new IllegalArgumentException("Synonym set ID cannot be null or blank");
            }
            this.synonymsSetId = synonymsSetId;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(synonymsSetId);
        }

        public String synonymsSetId() {
            return synonymsSetId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(synonymsSetId, request.synonymsSetId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(synonymsSetId);
        }
    }

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
            builder.field(ACKNOWLEDGED_KEY, acknowledgedResponse.isAcknowledged());
            builder.field("reload_analyzers_details");
            reloadAnalyzersResponse.toXContent(builder, params);
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
            Response response = (Response) o;
            return Objects.equals(acknowledgedResponse, response.acknowledgedResponse)
                && Objects.equals(reloadAnalyzersResponse, response.reloadAnalyzersResponse);
        }

        @Override
        public int hashCode() {
            return Objects.hash(acknowledgedResponse, reloadAnalyzersResponse);
        }
    }
}
