/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.watcher.rest.action;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.protocol.xpack.watcher.PutWatchRequest;
import org.elasticsearch.protocol.xpack.watcher.PutWatchResponse;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.security.rest.RestRequestFilter;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchAction;

import java.util.Collections;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.rest.RestStatus.CREATED;
import static org.elasticsearch.rest.RestStatus.OK;

public class RestPutWatchAction extends BaseRestHandler implements RestRequestFilter {

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(RestPutWatchAction.class));

    public RestPutWatchAction(RestController controller) {
        // TODO: remove deprecated endpoint in 8.0.0
        controller.registerWithDeprecatedHandler(
            POST, "/_watcher/watch/{id}", this,
            POST, "/_xpack/watcher/watch/{id}", deprecationLogger);
        controller.registerWithDeprecatedHandler(
            PUT, "/_watcher/watch/{id}", this,
            PUT, "/_xpack/watcher/watch/{id}", deprecationLogger);
    }

    @Override
    public String getName() {
        return "watcher_put_watch";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, NodeClient client) {
        PutWatchRequest putWatchRequest =
                new PutWatchRequest(request.param("id"), request.content(), request.getXContentType());
        putWatchRequest.setVersion(request.paramAsLong("version", Versions.MATCH_ANY));
        putWatchRequest.setIfSeqNo(request.paramAsLong("if_seq_no", putWatchRequest.getIfSeqNo()));
        putWatchRequest.setIfPrimaryTerm(request.paramAsLong("if_primary_term", putWatchRequest.getIfPrimaryTerm()));
        putWatchRequest.setActive(request.paramAsBoolean("active", putWatchRequest.isActive()));
        return channel -> client.execute(PutWatchAction.INSTANCE, putWatchRequest, new RestBuilderListener<>(channel) {
            @Override
            public RestResponse buildResponse(PutWatchResponse response, XContentBuilder builder) throws Exception {
                response.toXContent(builder, request);
                RestStatus status = response.isCreated() ? CREATED : OK;
                return new BytesRestResponse(status, builder);
            }
        });
    }

    private static final Set<String> FILTERED_FIELDS = Collections.unmodifiableSet(
            Sets.newHashSet("input.http.request.auth.basic.password", "input.chain.inputs.*.http.request.auth.basic.password",
                    "actions.*.email.attachments.*.reporting.auth.basic.password", "actions.*.webhook.auth.basic.password"));

    @Override
    public Set<String> getFilteredFields() {
        return FILTERED_FIELDS;
    }
}
