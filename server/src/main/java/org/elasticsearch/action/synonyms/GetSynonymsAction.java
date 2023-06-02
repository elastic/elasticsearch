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
import org.elasticsearch.synonyms.SynonymRule;
import org.elasticsearch.synonyms.SynonymsManagementAPIService;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class GetSynonymsAction extends ActionType<GetSynonymsAction.Response> {

    public static final GetSynonymsAction INSTANCE = new GetSynonymsAction();
    public static final String NAME = "cluster:admin/synonyms/get";

    public GetSynonymsAction() {
        super(NAME, Response::new);
    }

    public static class Request extends ActionRequest {
        private static final int MAX_SYNONYMS_RESULTS = 10_000;
        private final String synonymsSetId;
        private final int from;
        private final int size;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.synonymsSetId = in.readString();
            this.from = in.readVInt();
            this.size = in.readVInt();
        }

        public Request(String SynonymsSetId, int from, int size) {
            Objects.requireNonNull(SynonymsSetId, "Synonym set ID cannot be null");
            this.synonymsSetId = SynonymsSetId;
            this.from = from;
            this.size = size;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            validationException = validatePositiveInt("from", from, validationException);
            validationException = validatePositiveInt("size", size, validationException);

            if (from + size > MAX_SYNONYMS_RESULTS) {
                validationException = addValidationError(
                    "Too many synonyms to retrieve. [from] + [size] must be less than or equal to " + MAX_SYNONYMS_RESULTS,
                    validationException
                );
            }

            return validationException;
        }

        private ActionRequestValidationException validatePositiveInt(
            String paramName,
            int value,
            ActionRequestValidationException validationException
        ) {
            if (value < 0) {
                validationException = addValidationError("[" + paramName + "] must be a positive integer", validationException);
            }

            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(synonymsSetId);
            out.writeVInt(from);
            out.writeVInt(size);
        }

        public String synonymsSetId() {
            return synonymsSetId;
        }

        public int from() {
            return from;
        }

        public int size() {
            return size;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return from == request.from && size == request.size && Objects.equals(synonymsSetId, request.synonymsSetId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(synonymsSetId, from, size);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final SynonymsManagementAPIService.SynonymsSetResult synonymsSetResults;

        public Response(StreamInput in) throws IOException {
            super(in);
            this.synonymsSetResults = new SynonymsManagementAPIService.SynonymsSetResult(
                in.readVLong(),
                in.readArray(SynonymRule::new, SynonymRule[]::new)
            );
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
                builder.array(SynonymsManagementAPIService.SYNONYMS_SET_FIELD, (Object[]) synonymsSetResults.synonymRules());
            }
            builder.endObject();

            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(synonymsSetResults.totalSynonymRules());
            out.writeArray(synonymsSetResults.synonymRules());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(synonymsSetResults, response.synonymsSetResults);
        }

        @Override
        public int hashCode() {
            return Objects.hash(synonymsSetResults);
        }
    }
}
