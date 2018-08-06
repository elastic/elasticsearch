/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rollup.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.protocol.xpack.rollup.PutRollupJobRequest;
import org.elasticsearch.protocol.xpack.rollup.PutRollupJobResponse;

public class PutRollupJobAction extends Action<PutRollupJobResponse> {

    public static final PutRollupJobAction INSTANCE = new PutRollupJobAction();
    public static final String NAME = "cluster:admin/xpack/rollup/put";

    private PutRollupJobAction() {
        super(NAME);
    }

    @Override
    public PutRollupJobResponse newResponse() {
        return new PutRollupJobResponse();
    }

    public static class RequestBuilder
        extends MasterNodeOperationRequestBuilder<PutRollupJobRequest, PutRollupJobResponse, RequestBuilder> {

        protected RequestBuilder(ElasticsearchClient client, PutRollupJobAction action) {
            super(client, action, new PutRollupJobRequest());
        }
    }
}
