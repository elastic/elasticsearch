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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.synonyms.SynonymsManagementAPIService;
import org.elasticsearch.synonyms.SynonymsSet;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class GetSynonymsAction extends ActionType<GetSynonymsAction.Response> {

    public static final GetSynonymsAction INSTANCE = new GetSynonymsAction();
    public static final String NAME = "cluster:admin/synonyms/get";

    public GetSynonymsAction() {
        super(NAME, Response::new);
    }

    public static class Request extends ActionRequest {
        private final String SynonymsSetId;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.SynonymsSetId = in.readString();
        }

        public Request(String SynonymsSetId) {
            Objects.requireNonNull(SynonymsSetId, "Synonym set ID cannot be null");
            this.SynonymsSetId = SynonymsSetId;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(SynonymsSetId);
        }

        public String synonymsSetId() {
            return SynonymsSetId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(SynonymsSetId, request.SynonymsSetId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(SynonymsSetId);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        // TODO Pagination
        private final SynonymsManagementAPIService.SynonymsSetResult synonymsSetResults;

        public Response(StreamInput in) throws IOException {
            super(in);
            this.synonymsSetResults = new SynonymsManagementAPIService.SynonymsSetResult(in.readLong(), new SynonymsSet(in));
        }

        public Response(SynonymsManagementAPIService.SynonymsSetResult synonymsSetResult) {
            super();
            Objects.requireNonNull(synonymsSetResult, "Synonyms set result must not be null");
            this.synonymsSetResults = synonymsSetResult;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field("count", synonymsSetResults.totalSynonymRules());
                synonymsSetResults.synonymsSet().toXContent(builder, params);
            }
            builder.endObject();

            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(synonymsSetResults.totalSynonymRules());
            synonymsSetResults.synonymsSet().writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(this.synonymsSetResults, response.synonymsSetResults);
        }

        @Override
        public int hashCode() {
            return Objects.hash(synonymsSetResults);
        }
    }
}
