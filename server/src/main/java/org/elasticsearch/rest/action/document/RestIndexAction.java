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

package org.elasticsearch.rest.action.document;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.rest.action.RestStatusToXContentListener;

import java.io.IOException;
import java.util.Locale;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestIndexAction extends BaseRestHandler {
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(
        LogManager.getLogger(RestDeleteAction.class));
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Specifying types in document " +
        "index requests is deprecated, use the typeless endpoints instead (/{index}/_doc/{id}, /{index}/_doc, " +
        "or /{index}/_create/{id}).";

    public RestIndexAction(RestController controller) {
        controller.registerHandler(POST, "/{index}/_doc", this); // auto id creation
        controller.registerHandler(PUT, "/{index}/_doc/{id}", this);
        controller.registerHandler(POST, "/{index}/_doc/{id}", this);

        CreateHandler createHandler = new CreateHandler();
        controller.registerHandler(PUT, "/{index}/_create/{id}", createHandler);
        controller.registerHandler(POST, "/{index}/_create/{id}/", createHandler);

        // Deprecated typed endpoints.
        controller.registerHandler(POST, "/{index}/{type}", this); // auto id creation
        controller.registerHandler(PUT, "/{index}/{type}/{id}", this);
        controller.registerHandler(POST, "/{index}/{type}/{id}", this);
        controller.registerHandler(PUT, "/{index}/{type}/{id}/_create", createHandler);
        controller.registerHandler(POST, "/{index}/{type}/{id}/_create", createHandler);
    }

    @Override
    public String getName() {
        return "document_index_action";
    }

    final class CreateHandler extends BaseRestHandler {
        protected CreateHandler() {
        }

        @Override
        public String getName() {
            return "document_create_action";
        }

        @Override
        public RestChannelConsumer prepareRequest(RestRequest request, final NodeClient client) throws IOException {
            validateOpType(request.params().get("op_type"));
            request.params().put("op_type", "create");
            return RestIndexAction.this.prepareRequest(request, client);
        }

        void validateOpType(String opType) {
            if (null != opType && false == "create".equals(opType.toLowerCase(Locale.ROOT))) {
                throw new IllegalArgumentException("opType must be 'create', found: [" + opType + "]");
            }
        }
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        IndexRequest indexRequest;
        final String type = request.param("type");
        if (type != null && type.equals(MapperService.SINGLE_MAPPING_NAME) == false) {
            deprecationLogger.deprecatedAndMaybeLog("index_with_types", TYPES_DEPRECATION_MESSAGE);
            indexRequest = new IndexRequest(request.param("index"), type, request.param("id"));
        } else {
            indexRequest = new IndexRequest(request.param("index"));
            indexRequest.id(request.param("id"));
        }
        indexRequest.routing(request.param("routing"));
        indexRequest.setPipeline(request.param("pipeline"));
        indexRequest.source(request.requiredContent(), request.getXContentType());
        indexRequest.timeout(request.paramAsTime("timeout", IndexRequest.DEFAULT_TIMEOUT));
        indexRequest.setRefreshPolicy(request.param("refresh"));
        indexRequest.version(RestActions.parseVersion(request));
        indexRequest.versionType(VersionType.fromString(request.param("version_type"), indexRequest.versionType()));
        indexRequest.setIfSeqNo(request.paramAsLong("if_seq_no", indexRequest.ifSeqNo()));
        indexRequest.setIfPrimaryTerm(request.paramAsLong("if_primary_term", indexRequest.ifPrimaryTerm()));
        String sOpType = request.param("op_type");
        String waitForActiveShards = request.param("wait_for_active_shards");
        if (waitForActiveShards != null) {
            indexRequest.waitForActiveShards(ActiveShardCount.parseString(waitForActiveShards));
        }
        if (sOpType != null) {
            indexRequest.opType(sOpType);
        }

        return channel ->
                client.index(indexRequest, new RestStatusToXContentListener<>(channel, r -> r.getLocation(indexRequest.routing())));
    }

}
