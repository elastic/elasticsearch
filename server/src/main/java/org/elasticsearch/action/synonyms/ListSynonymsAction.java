/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.synonyms;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.synonyms.SynonymSetResult;
import org.elasticsearch.synonyms.SynonymsManagementAPIService;

import java.io.IOException;
import java.util.function.IntFunction;

public class ListSynonymsAction extends AbstractSynonymsRetrievalAction<ListSynonymsAction.Response> {

    public static final ListSynonymsAction INSTANCE = new ListSynonymsAction();
    public static final String NAME = "cluster:admin/synonyms/list";

    public ListSynonymsAction() {
        super(NAME, Response::new);
    }

    public static class Request extends AbstractSynonymsRetrievalAction.Request {
        public Request(StreamInput in) throws IOException {
            super(in);
        }

        public Request(int from, int size) {
            super(from, size);
        }
    }

    public static class Response extends AbstractSynonymsRetrievalAction.AbstractPagedResultResponse<SynonymSetResult> {

        public static final String RESULTS_FIELD = "results";

        public Response(StreamInput in) throws IOException {
            super(in);
        }

        public Response(SynonymsManagementAPIService.PagedResult<SynonymSetResult> result) {
            super(result);
        }

        @Override
        protected String resultFieldName() {
            return RESULTS_FIELD;
        }

        @Override
        protected Reader<SynonymSetResult> reader() {
            return SynonymSetResult::new;
        }

        @Override
        protected IntFunction<SynonymSetResult[]> arraySupplier() {
            return SynonymSetResult[]::new;
        }
    }

}
