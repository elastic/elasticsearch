/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.synonyms;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.synonyms.PagedResult;
import org.elasticsearch.synonyms.SynonymSetSummary;

import java.io.IOException;
import java.util.function.IntFunction;

public class GetSynonymsSetsAction extends AbstractSynonymsPagedResultAction<GetSynonymsSetsAction.Response> {

    public static final GetSynonymsSetsAction INSTANCE = new GetSynonymsSetsAction();
    public static final String NAME = "cluster:admin/synonyms/list";

    public GetSynonymsSetsAction() {
        super(NAME, Response::new);
    }

    public static class Request extends AbstractSynonymsPagedResultAction.Request {
        public Request(StreamInput in) throws IOException {
            super(in);
        }

        public Request(int from, int size) {
            super(from, size);
        }
    }

    public static class Response extends AbstractSynonymsPagedResultAction.AbstractPagedResultResponse<SynonymSetSummary> {

        public static final String RESULTS_FIELD = "results";

        public Response(StreamInput in) throws IOException {
            super(in);
        }

        public Response(PagedResult<SynonymSetSummary> result) {
            super(result);
        }

        @Override
        protected String resultFieldName() {
            return RESULTS_FIELD;
        }

        @Override
        protected Reader<SynonymSetSummary> reader() {
            return SynonymSetSummary::new;
        }

        @Override
        protected IntFunction<SynonymSetSummary[]> arraySupplier() {
            return SynonymSetSummary[]::new;
        }
    }

}
