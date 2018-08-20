/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.indexing.IndexerJobStats;
import org.elasticsearch.xpack.core.rollup.action.GetRollupJobsAction;
import org.elasticsearch.xpack.rollup.Rollup;

public class RestGetRollupJobsAction extends BaseRestHandler {
    public static final ParseField ID = new ParseField("id");

    public RestGetRollupJobsAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.GET, Rollup.BASE_PATH + "job/{id}/", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        String id = restRequest.param(ID.getPreferredName());
        boolean rollupsIndexedFormat = restRequest.paramAsBoolean(IndexerJobStats.ROLLUP_BWC_XCONTENT_PARAM, true);
        GetRollupJobsAction.Request request = new GetRollupJobsAction.Request(id);

        return channel -> {
            // BWC: inject parameter to force the old style rollup output
            // injecting after request handling avoids IAE
            channel.request().params().put(IndexerJobStats.ROLLUP_BWC_XCONTENT_PARAM, String.valueOf(rollupsIndexedFormat));
            client.execute(GetRollupJobsAction.INSTANCE, request, new RestToXContentListener<>(channel));
        };
    }

    @Override
    public String getName() {
        return "rollup_get_job_action";
    }
}
