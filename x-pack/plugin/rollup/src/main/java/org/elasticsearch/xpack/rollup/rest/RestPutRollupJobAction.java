/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.rest;


import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.rollup.action.PutRollupJobAction;
import org.elasticsearch.xpack.rollup.Rollup;

import java.io.IOException;

public class RestPutRollupJobAction extends BaseRestHandler {

    public RestPutRollupJobAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.PUT, Rollup.BASE_PATH +  "job/{id}/", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final String id = request.param("id");
        final PutRollupJobAction.Request putRollupJobRequest = PutRollupJobAction.Request.fromXContent(request.contentParser(), id);
        return channel -> client.execute(PutRollupJobAction.INSTANCE, putRollupJobRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "rollup_put_job_action";
    }
}
