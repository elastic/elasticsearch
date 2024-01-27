/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.synonyms;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.synonyms.PagedResult;
import org.elasticsearch.synonyms.SynonymRule;
import org.elasticsearch.synonyms.SynonymsManagementAPIService;

import java.io.IOException;
import java.util.Objects;
import java.util.function.IntFunction;

public class GetSynonymsAction extends AbstractSynonymsPagedResultAction<GetSynonymsAction.Response> {

    public static final GetSynonymsAction INSTANCE = new GetSynonymsAction();
    public static final String NAME = "cluster:admin/synonyms/get";

    public GetSynonymsAction() {
        super(NAME, Response::new);
    }

    public static class Request extends AbstractSynonymsPagedResultAction.Request {
        private final String synonymsSetId;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.synonymsSetId = in.readString();
        }

        public Request(String synonymsSetId, int from, int size) {
            super(from, size);
            Objects.requireNonNull(synonymsSetId, "Synonym set ID cannot be null");
            this.synonymsSetId = synonymsSetId;
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
            if (super.equals(o) == false) return false;
            Request request = (Request) o;
            return Objects.equals(synonymsSetId, request.synonymsSetId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), synonymsSetId);
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
    }
}
