/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search.persistent;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.persistent.PersistentSearchStorageService;
import org.elasticsearch.search.persistent.ShardSearchResult;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class GetShardResultAction extends ActionType<GetShardResultAction.Response> {
    public static String NAME = "indices:data/read/persistent_search/get_shard_result";
    public static GetShardResultAction INSTANCE = new GetShardResultAction();

    public GetShardResultAction() {
        super(NAME, Response::new);
    }

    public static class TransportAction extends HandledTransportAction<Request, Response> {
        private final PersistentSearchStorageService persistentSearchStorageService;

        @Inject
        public TransportAction(TransportService transportService,
                               ActionFilters actionFilters,
                               PersistentSearchStorageService persistentSearchStorageService) {
            super(NAME, transportService, actionFilters, Request::new);
            this.persistentSearchStorageService = persistentSearchStorageService;
        }

        @Override
        protected void doExecute(Task task,
                                 Request request,
                                 ActionListener<Response> listener) {
            persistentSearchStorageService.getShardResult(request.shardResultId, listener.map(Response::new));
        }
    }

    public static class Request extends ActionRequest {
        private final String shardResultId;

        public Request(String shardResultId) {
            this.shardResultId = shardResultId;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.shardResultId = in.readString();
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(shardResultId);
        }
    }

    public static class Response extends ActionResponse {
        private final ShardSearchResult shardSearchResult;

        public Response(ShardSearchResult shardSearchResult) {
            this.shardSearchResult = shardSearchResult;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            this.shardSearchResult = new ShardSearchResult(in);
        }

        public ShardSearchResult getShardSearchResult() {
            return shardSearchResult;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            shardSearchResult.writeTo(out);
        }
    }
}
