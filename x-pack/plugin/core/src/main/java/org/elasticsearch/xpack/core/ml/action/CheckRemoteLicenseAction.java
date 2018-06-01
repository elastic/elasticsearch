/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

public class CheckRemoteLicenseAction extends Action<CheckRemoteLicenseAction.Request, CheckRemoteLicenseAction.Response, CheckRemoteLicenseAction.RequestBuilder> {

    public static final CheckRemoteLicenseAction INSTANCE = new CheckRemoteLicenseAction();
    public static final String NAME = "cluster:admin/xpack/ml/datafeed/checklic";

    private CheckRemoteLicenseAction() {
        super(NAME);
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client, this);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends AcknowledgedRequest<Request> {

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class Response extends AcknowledgedResponse {

        public Response() {
            super();
        }
    }

    public static class RequestBuilder extends MasterNodeOperationRequestBuilder<Request, Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, CheckRemoteLicenseAction action) {
            super(client, action, new Request());
        }
    }

}
