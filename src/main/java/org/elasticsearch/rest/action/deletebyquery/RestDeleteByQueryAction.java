/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.rest.action.deletebyquery;

import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.deletebyquery.IndexDeleteByQueryResponse;
import org.elasticsearch.action.deletebyquery.ShardDeleteByQueryRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.QuerySourceBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestBuilderListener;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

/**
 *
 */
public class RestDeleteByQueryAction extends BaseRestHandler {

    @Inject
    public RestDeleteByQueryAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(DELETE, "/{index}/_query", this);
        controller.registerHandler(DELETE, "/{index}/{type}/_query", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest(Strings.splitStringByCommaToArray(request.param("index")));
        deleteByQueryRequest.listenerThreaded(false);
        if (RestActions.hasBodyContent(request)) {
            deleteByQueryRequest.source(RestActions.getRestContent(request));
        } else {
            QuerySourceBuilder querySourceBuilder = RestActions.parseQuerySource(request);
            if (querySourceBuilder != null) {
                deleteByQueryRequest.source(querySourceBuilder);
            }
        }
        deleteByQueryRequest.types(Strings.splitStringByCommaToArray(request.param("type")));
        deleteByQueryRequest.timeout(request.paramAsTime("timeout", ShardDeleteByQueryRequest.DEFAULT_TIMEOUT));

        deleteByQueryRequest.routing(request.param("routing"));
        String consistencyLevel = request.param("consistency");
        if (consistencyLevel != null) {
            deleteByQueryRequest.consistencyLevel(WriteConsistencyLevel.fromString(consistencyLevel));
        }
        deleteByQueryRequest.indicesOptions(IndicesOptions.fromRequest(request, deleteByQueryRequest.indicesOptions()));
        client.deleteByQuery(deleteByQueryRequest, new RestBuilderListener<DeleteByQueryResponse>(channel) {
            @Override
            public RestResponse buildResponse(DeleteByQueryResponse result, XContentBuilder builder) throws Exception {
                RestStatus restStatus = result.status();
                builder.startObject();
                builder.startObject(Fields._INDICES);
                for (IndexDeleteByQueryResponse indexDeleteByQueryResponse : result.getIndices().values()) {
                    builder.startObject(indexDeleteByQueryResponse.getIndex(), XContentBuilder.FieldCaseConversion.NONE);
                    indexDeleteByQueryResponse.getShardInfo().toXContent(builder, request);
                    builder.endObject();
                    builder.endObject();
                }
                builder.endObject();
                return new BytesRestResponse(restStatus, builder);
            }
        });
    }

    static final class Fields {
        static final XContentBuilderString _INDICES = new XContentBuilderString("_indices");
        static final XContentBuilderString _SHARDS = new XContentBuilderString("_shards");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString SUCCESSFUL = new XContentBuilderString("successful");
        static final XContentBuilderString FAILED = new XContentBuilderString("failed");
        static final XContentBuilderString FAILURES = new XContentBuilderString("failures");
        static final XContentBuilderString INDEX = new XContentBuilderString("index");
        static final XContentBuilderString SHARD = new XContentBuilderString("shard");
        static final XContentBuilderString REASON = new XContentBuilderString("reason");
    }
}
