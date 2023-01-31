/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.ingest;

import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestPutPipelineAction extends BaseRestHandler {
    public static final String PROCESSORS_KEY = "processors";

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

        Tuple<XContentType, BytesReference> sourceTuple = restRequest.contentOrSourceParam();
        PutPipelineRequest request = new PutPipelineRequest(restRequest.param("id"), sourceTuple.v2(), sourceTuple.v1(), ifVersion);

        // check for nested processors in the config
        Map<String, Object> config = XContentHelper.convertToMap(request.getSource(), false, request.getXContentType()).v2();
        if (Pipeline.containsNestedProcessors(config)) {
            throw new IllegalArgumentException("[processors] contains nested objects but should be a list of single-entry objects");
        }

        request.masterNodeTimeout(restRequest.paramAsTime("master_timeout", request.masterNodeTimeout()));
        request.timeout(restRequest.paramAsTime("timeout", request.timeout()));
        return channel -> client.admin().cluster().putPipeline(request, new RestToXContentListener<>(channel));
    }
}
