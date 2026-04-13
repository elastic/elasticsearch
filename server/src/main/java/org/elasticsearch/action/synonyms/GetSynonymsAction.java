/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.synonyms;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.synonyms.PagedResult;
import org.elasticsearch.synonyms.SynonymRule;
import org.elasticsearch.synonyms.SynonymsManagementAPIService;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;
import java.util.function.IntFunction;

public class GetSynonymsAction extends AbstractSynonymsPagedResultAction<GetSynonymsAction.Response> {

    public static final GetSynonymsAction INSTANCE = new GetSynonymsAction();
    public static final String NAME = "cluster:admin/synonyms/get";

    static final TransportVersion SYNONYMS_GET_PIT = TransportVersion.fromName("synonyms_get_pit_search_after");

    public GetSynonymsAction() {
        super(NAME, Response::new);
    }

    public static class Request extends AbstractSynonymsPagedResultAction.Request {
        private final String synonymsSetId;
        /**
         * True when this request should use PIT + search_after pagination. When {@code true}, {@code from}
         * is always 0 and the response will carry cursor fields. When {@code false}, legacy offset pagination
         * is used and no cursor fields are returned.
         */
        private final boolean usePit;
        private final String pitId;
        private final String searchAfter;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.synonymsSetId = in.readString();
            if (in.getTransportVersion().supports(SYNONYMS_GET_PIT)) {
                this.usePit = in.readBoolean();
                this.pitId = in.readOptionalString();
                this.searchAfter = in.readOptionalString();
            } else {
                this.usePit = false;
                this.pitId = null;
                this.searchAfter = null;
            }
        }

        public Request(String synonymsSetId, int from, int size) {
            super(from, size);
            Objects.requireNonNull(synonymsSetId, "Synonym set ID cannot be null");
            this.synonymsSetId = synonymsSetId;
            this.usePit = false;
            this.pitId = null;
            this.searchAfter = null;
        }

        /**
         * Constructs a PIT-based paginated request. {@code from} is always 0 when using PIT + search_after.
         * Pass {@code null} for {@code pitId} on the first page to open a new PIT; pass the {@code pit_id}
         * returned from a previous response to continue iterating.
         */
        public Request(String synonymsSetId, int size, String pitId, String searchAfter) {
            super(0, size);
            Objects.requireNonNull(synonymsSetId, "Synonym set ID cannot be null");
            this.synonymsSetId = synonymsSetId;
            this.usePit = true;
            this.pitId = pitId;
            this.searchAfter = searchAfter;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(synonymsSetId);
            if (out.getTransportVersion().supports(SYNONYMS_GET_PIT)) {
                out.writeBoolean(usePit);
                out.writeOptionalString(pitId);
                out.writeOptionalString(searchAfter);
            }
        }

        public String synonymsSetId() {
            return synonymsSetId;
        }

        public boolean usePit() {
            return usePit;
        }

        public String pitId() {
            return pitId;
        }

        public String searchAfter() {
            return searchAfter;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (super.equals(o) == false) return false;
            Request request = (Request) o;
            return usePit == request.usePit
                && Objects.equals(synonymsSetId, request.synonymsSetId)
                && Objects.equals(pitId, request.pitId)
                && Objects.equals(searchAfter, request.searchAfter);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), synonymsSetId, usePit, pitId, searchAfter);
        }
    }

    public static class Response extends AbstractPagedResultResponse<SynonymRule> {
        private final String nextPitId;
        private final String nextSearchAfter;

        public Response(StreamInput in) throws IOException {
            super(in);
            if (in.getTransportVersion().supports(SYNONYMS_GET_PIT)) {
                this.nextPitId = in.readOptionalString();
                this.nextSearchAfter = in.readOptionalString();
            } else {
                this.nextPitId = null;
                this.nextSearchAfter = null;
            }
        }

        public Response(PagedResult<SynonymRule> result) {
            super(result);
            this.nextPitId = null;
            this.nextSearchAfter = null;
        }

        public Response(PagedResult<SynonymRule> result, String nextPitId, String nextSearchAfter) {
            super(result);
            this.nextPitId = nextPitId;
            this.nextSearchAfter = nextSearchAfter;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (out.getTransportVersion().supports(SYNONYMS_GET_PIT)) {
                out.writeOptionalString(nextPitId);
                out.writeOptionalString(nextSearchAfter);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field("count", totalCount);
                builder.array(resultFieldName(), (Object[]) resultList);
                if (nextPitId != null) {
                    builder.field("pit_id", nextPitId);
                }
                if (nextSearchAfter != null) {
                    builder.field("next_search_after", nextSearchAfter);
                }
            }
            builder.endObject();
            return builder;
        }

        public String nextPitId() {
            return nextPitId;
        }

        public String nextSearchAfter() {
            return nextSearchAfter;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (super.equals(o) == false) return false;
            Response response = (Response) o;
            return Objects.equals(nextPitId, response.nextPitId) && Objects.equals(nextSearchAfter, response.nextSearchAfter);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), nextPitId, nextSearchAfter);
        }

        @Override
        protected String resultFieldName() {
            return SynonymsManagementAPIService.SYNONYMS_SET_FIELD;
        }

        @Override
        protected Reader<SynonymRule> reader() {
            return SynonymRule::new;
        }

        @Override
        protected IntFunction<SynonymRule[]> arraySupplier() {
            return SynonymRule[]::new;
        }

        @SuppressWarnings("unchecked")
        @Override
        PagedResult<SynonymRule> getResults() {
            return (PagedResult<SynonymRule>) super.getResults();
        }
    }
}
