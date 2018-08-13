/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rollup.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.protocol.xpack.rollup.GetRollupCapsRequest;
import org.elasticsearch.protocol.xpack.rollup.GetRollupCapsResponse;

public class GetRollupCapsAction extends Action<GetRollupCapsResponse> {

    public static final GetRollupCapsAction INSTANCE = new GetRollupCapsAction();
    public static final String NAME = "cluster:monitor/xpack/rollup/get/caps";
    public static final ParseField CONFIG = new ParseField("config");
    public static final ParseField STATUS = new ParseField("status");

    private GetRollupCapsAction() {
        super(NAME);
    }

    @Override
    public GetRollupCapsResponse newResponse() {
        return new GetRollupCapsResponse();
    }

    public static class RequestBuilder extends ActionRequestBuilder<GetRollupCapsRequest, GetRollupCapsResponse> {

        protected RequestBuilder(ElasticsearchClient client, GetRollupCapsAction action) {
            super(client, action, new GetRollupCapsRequest());
        }
    }
}
