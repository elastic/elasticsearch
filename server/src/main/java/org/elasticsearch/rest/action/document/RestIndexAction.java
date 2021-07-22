/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.document;

import org.elasticsearch.Version;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.rest.action.RestStatusToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestIndexAction extends BaseRestHandler {
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Specifying types in document "
        + "index requests is deprecated, use the typeless endpoints instead (/{index}/_doc/{id}, /{index}/_doc, "
        + "or /{index}/_create/{id}).";

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/{index}/_doc/{id}"),
            new Route(PUT, "/{index}/_doc/{id}"),
            Route.builder(POST, "/{index}/{type}/{id}")
                .deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7)
                .build(),
            Route.builder(PUT, "/{index}/{type}/{id}")
                .deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7)
                .build());
    }

    @Override
    public String getName() {
        return "document_index_action";
    }

    public static final class CreateHandler extends RestIndexAction {

        @Override
        public String getName() {
            return "document_create_action";
        }

        @Override
        public List<Route> routes() {
            return List.of(
                new Route(POST, "/{index}/_create/{id}"),
                new Route(PUT, "/{index}/_create/{id}"),
                Route.builder(POST, "/{index}/{type}/{id}/_create")
                    .deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7)
                    .build(),
                Route.builder(PUT, "/{index}/{type}/{id}/_create")
                    .deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7)
                    .build());
        }

        @Override
        public RestChannelConsumer prepareRequest(RestRequest request, final NodeClient client) throws IOException {
            validateOpType(request.params().get("op_type"));
            request.params().put("op_type", "create");
            return super.prepareRequest(request, client);
        }

        void validateOpType(String opType) {
            if (null != opType && false == "create".equals(opType.toLowerCase(Locale.ROOT))) {
                throw new IllegalArgumentException("opType must be 'create', found: [" + opType + "]");
            }
        }
    }

    public static final class AutoIdHandler extends RestIndexAction {

        private final Supplier<DiscoveryNodes> nodesInCluster;

        public AutoIdHandler(Supplier<DiscoveryNodes> nodesInCluster) {
            this.nodesInCluster = nodesInCluster;
        }

        @Override
        public String getName() {
            return "document_create_action_auto_id";
        }

        @Override
        public List<Route> routes() {
            return List.of(
                new Route(POST, "/{index}/_doc"),
                Route.builder(POST, "/{index}/{type}")
                    .deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7)
                    .build());
        }

        @Override
        public RestChannelConsumer prepareRequest(RestRequest request, final NodeClient client) throws IOException {
            assert request.params().get("id") == null : "non-null id: " + request.params().get("id");
            if (request.params().get("op_type") == null && nodesInCluster.get().getMinNodeVersion().onOrAfter(Version.V_7_5_0)) {
                // default to op_type create
                request.params().put("op_type", "create");
            }
            return super.prepareRequest(request, client);
        }
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        if (request.getRestApiVersion() == RestApiVersion.V_7) {
            request.param("type"); // consume and ignore the type
        }

        IndexRequest indexRequest = new IndexRequest(request.param("index"));
        indexRequest.id(request.param("id"));
        indexRequest.routing(request.param("routing"));
        indexRequest.setPipeline(request.param("pipeline"));
        indexRequest.source(request.requiredContent(), request.getXContentType());
        indexRequest.timeout(request.paramAsTime("timeout", IndexRequest.DEFAULT_TIMEOUT));
        indexRequest.setRefreshPolicy(request.param("refresh"));
        indexRequest.version(RestActions.parseVersion(request));
        indexRequest.versionType(VersionType.fromString(request.param("version_type"), indexRequest.versionType()));
        indexRequest.setIfSeqNo(request.paramAsLong("if_seq_no", indexRequest.ifSeqNo()));
        indexRequest.setIfPrimaryTerm(request.paramAsLong("if_primary_term", indexRequest.ifPrimaryTerm()));
        indexRequest.setRequireAlias(request.paramAsBoolean(DocWriteRequest.REQUIRE_ALIAS, indexRequest.isRequireAlias()));
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
