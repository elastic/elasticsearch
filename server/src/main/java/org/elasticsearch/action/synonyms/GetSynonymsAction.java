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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.synonyms.PagedResult;
import org.elasticsearch.synonyms.SynonymRule;
import org.elasticsearch.synonyms.SynonymsManagementAPIService;

import java.io.IOException;
import java.util.Objects;
import java.util.function.IntFunction;

public class GetSynonymsAction extends AbstractSynonymsPagedResultAction<GetSynonymsAction.Response> {

    public static final GetSynonymsAction INSTANCE = new GetSynonymsAction();
    public static final String NAME = "cluster:admin/synonyms/get";

    static final TransportVersion SYNONYMS_GET_SEARCH_AFTER = TransportVersion.fromName("synonyms_get_search_after");

    public GetSynonymsAction() {
        super(NAME, Response::new);
    }

    public static class Request extends AbstractSynonymsPagedResultAction.Request {
        private final String synonymsSetId;
        @Nullable
        private final String searchAfter;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.synonymsSetId = in.readString();
            if (in.getTransportVersion().supports(SYNONYMS_GET_SEARCH_AFTER)) {
                this.searchAfter = in.readOptionalString();
            } else {
                this.searchAfter = null;
            }
        }

        /** Legacy offset-based request. */
        public Request(String synonymsSetId, int from, int size) {
            super(from, size);
            Objects.requireNonNull(synonymsSetId, "Synonym set ID cannot be null");
            this.synonymsSetId = synonymsSetId;
            this.searchAfter = null;
        }

        /** Cursor-based request. {@code searchAfter} is {@code null} on the first page. */
        public Request(String synonymsSetId, @Nullable String searchAfter, int size) {
            super(0, size);
            Objects.requireNonNull(synonymsSetId, "Synonym set ID cannot be null");
            this.synonymsSetId = synonymsSetId;
            this.searchAfter = searchAfter;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(synonymsSetId);
            if (out.getTransportVersion().supports(SYNONYMS_GET_SEARCH_AFTER)) {
                out.writeOptionalString(searchAfter);
            }
        }

        public String synonymsSetId() {
            return synonymsSetId;
        }

        @Nullable
        public String searchAfter() {
            return searchAfter;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (super.equals(o) == false) return false;
            Request request = (Request) o;
            return Objects.equals(synonymsSetId, request.synonymsSetId) && Objects.equals(searchAfter, request.searchAfter);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), synonymsSetId, searchAfter);
        }
    }

    public static class Response extends AbstractPagedResultResponse<SynonymRule> {

        public Response(StreamInput in) throws IOException {
            super(in);
        }

        public Response(PagedResult<SynonymRule> result) {
            super(result);
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
