/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.document;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.rest.action.document.RestBulkAction.FAILURE_STORE_STATUS_CAPABILITY;

@ServerlessScope(Scope.PUBLIC)
public class RestIndexAction extends BaseRestHandler {
    static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Specifying types in document "
        + "index requests is deprecated, use the typeless endpoints instead (/{index}/_doc/{id}, /{index}/_doc, "
        + "or /{index}/_create/{id}).";
    private final Set<String> capabilities = Set.of(FAILURE_STORE_STATUS_CAPABILITY);

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/{index}/_doc/{id}"), new Route(PUT, "/{index}/_doc/{id}"));
    }

    @Override
    public String getName() {
        return "document_index_action";
    }

    @ServerlessScope(Scope.PUBLIC)
    public static final class CreateHandler extends RestIndexAction {

        @Override
        public String getName() {
            return "document_create_action";
        }

        @Override
        public List<Route> routes() {
            return List.of(new Route(POST, "/{index}/_create/{id}"), new Route(PUT, "/{index}/_create/{id}"));
        }

        @Override
        public RestChannelConsumer prepareRequest(RestRequest request, final NodeClient client) throws IOException {
            validateOpType(request.params().get("op_type"));
            request.params().put("op_type", "create");
            return super.prepareRequest(request, client);
        }

        static void validateOpType(String opType) {
            if (null != opType && false == "create".equals(opType.toLowerCase(Locale.ROOT))) {
                throw new IllegalArgumentException("opType must be 'create', found: [" + opType + "]");
            }
        }
    }

    @ServerlessScope(Scope.PUBLIC)
    public static final class AutoIdHandler extends RestIndexAction {

        public AutoIdHandler() {}

        @Override
        public String getName() {
            return "document_create_action_auto_id";
        }

        @Override
        public List<Route> routes() {
            return List.of(new Route(POST, "/{index}/_doc"));
        }

        @Override
        public RestChannelConsumer prepareRequest(RestRequest request, final NodeClient client) throws IOException {
            assert request.params().get("id") == null : "non-null id: " + request.params().get("id");
            // default to op_type create
            request.params().putIfAbsent("op_type", "create");
            return super.prepareRequest(request, client);
        }
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        ReleasableBytesReference source = request.requiredContent();
        IndexRequest indexRequest = new IndexRequest(request.param("index"));
        indexRequest.id(request.param("id"));
        indexRequest.routing(request.param("routing"));
        indexRequest.setPipeline(request.param("pipeline"));
        indexRequest.source(source, request.getXContentType());
        indexRequest.timeout(request.paramAsTime("timeout", IndexRequest.DEFAULT_TIMEOUT));
        indexRequest.setRefreshPolicy(request.param("refresh"));
        indexRequest.version(RestActions.parseVersion(request));
        indexRequest.versionType(VersionType.fromString(request.param("version_type"), indexRequest.versionType()));
        indexRequest.setIfSeqNo(request.paramAsLong("if_seq_no", indexRequest.ifSeqNo()));
        indexRequest.setIfPrimaryTerm(request.paramAsLong("if_primary_term", indexRequest.ifPrimaryTerm()));
        indexRequest.setRequireAlias(request.paramAsBoolean(DocWriteRequest.REQUIRE_ALIAS, indexRequest.isRequireAlias()));
        indexRequest.setRequireDataStream(request.paramAsBoolean(DocWriteRequest.REQUIRE_DATA_STREAM, indexRequest.isRequireDataStream()));
        indexRequest.setIncludeSourceOnError(RestUtils.getIncludeSourceOnError(request));
        String sOpType = request.param("op_type");
        String waitForActiveShards = request.param("wait_for_active_shards");
        if (waitForActiveShards != null) {
            indexRequest.waitForActiveShards(ActiveShardCount.parseString(waitForActiveShards));
        }
        if (sOpType != null) {
            indexRequest.opType(sOpType);
        }

        return channel -> {
            source.mustIncRef();
            client.index(
                indexRequest,
                ActionListener.releaseAfter(
                    new RestToXContentListener<>(channel, DocWriteResponse::status, r -> r.getLocation(indexRequest.routing())),
                    source
                )
            );
        };
    }

    @Override
    public Set<String> supportedCapabilities() {
        return capabilities;
    }
}
