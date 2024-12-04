/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.ingest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.ingest.PutPipelineTransportAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.rest.RestUtils.getAckTimeout;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

@ServerlessScope(Scope.PUBLIC)
public class RestPutPipelineAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_ingest/pipeline/{id}"));
    }

    @Override
    public String getName() {
        return "ingest_put_pipeline_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        Integer ifVersion = null;
        if (restRequest.hasParam("if_version")) {
            String versionString = restRequest.param("if_version");
            try {
                ifVersion = Integer.parseInt(versionString);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "invalid value [%s] specified for [if_version]. must be an integer value", versionString)
                );
            }
        }

        Tuple<XContentType, ReleasableBytesReference> sourceTuple = restRequest.contentOrSourceParam();
        var content = sourceTuple.v2();
        final var request = new PutPipelineRequest(
            getMasterNodeTimeout(restRequest),
            getAckTimeout(restRequest),
            restRequest.param("id"),
            content,
            sourceTuple.v1(),
            ifVersion
        );
        return channel -> client.execute(
            PutPipelineTransportAction.TYPE,
            request,
            ActionListener.withRef(new RestToXContentListener<>(channel), content)
        );
    }
}
