/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.rest.action.percolate;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.percolate.PercolateRequest;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.BaseActionRequestRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestToXContentListener;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 *
 */
public class RestPercolateAction extends BaseActionRequestRestHandler<PercolateRequest> {

    @Inject
    public RestPercolateAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/{index}/{type}/_percolate", this);
        controller.registerHandler(POST, "/{index}/{type}/_percolate", this);

        RestPercolateExistingDocHandler percolateExistingDocHandler = new RestPercolateExistingDocHandler(settings, client);
        controller.registerHandler(GET, "/{index}/{type}/{id}/_percolate", percolateExistingDocHandler);
        controller.registerHandler(POST, "/{index}/{type}/{id}/_percolate", percolateExistingDocHandler);

        RestCountPercolateDocHandler countHandler = new RestCountPercolateDocHandler(settings, client);
        controller.registerHandler(GET, "/{index}/{type}/_percolate/count", countHandler);
        controller.registerHandler(POST, "/{index}/{type}/_percolate/count", countHandler);

        RestCountPercolateExistingDocHandler countExistingDocHandler = new RestCountPercolateExistingDocHandler(settings, client);
        controller.registerHandler(GET, "/{index}/{type}/{id}/_percolate/count", countExistingDocHandler);
        controller.registerHandler(POST, "/{index}/{type}/{id}/_percolate/count", countExistingDocHandler);
    }

    private RestPercolateAction(Settings settings, Client client) {
        super(settings, client);
    }

    @Override
    protected PercolateRequest newRequest(RestRequest request) {
        PercolateRequest percolateRequest = new PercolateRequest();
        percolateRequest.indices(Strings.splitStringByCommaToArray(request.param("index")));
        percolateRequest.documentType(request.param("type"));
        percolateRequest.routing(request.param("routing"));
        percolateRequest.preference(request.param("preference"));
        percolateRequest.source(RestActions.getRestContent(request), request.contentUnsafe());
        percolateRequest.indicesOptions(IndicesOptions.fromRequest(request, percolateRequest.indicesOptions()));
        return percolateRequest;
    }

    @Override
    protected void doHandleRequest(RestRequest restRequest, RestChannel restChannel, PercolateRequest request) {
        // we just send a response, no need to fork
        request.listenerThreaded(false);
        client.percolate(request, new RestToXContentListener<PercolateResponse>(restChannel));
    }

    final static class RestCountPercolateDocHandler extends RestPercolateAction {

        private RestCountPercolateDocHandler(Settings settings, Client client) {
            super(settings, client);
        }

        @Override
        protected PercolateRequest newRequest(RestRequest request) {
            PercolateRequest percolateRequest = super.newRequest(request);
            percolateRequest.onlyCount(true);
            return percolateRequest;
        }
    }

    private static class RestPercolateExistingDocHandler extends RestPercolateAction {

        private RestPercolateExistingDocHandler(Settings settings, Client client) {
            super(settings, client);
        }

        @Override
        protected PercolateRequest newRequest(RestRequest request) {
            PercolateRequest percolateRequest = new PercolateRequest();
            String index = request.param("index");
            String type = request.param("type");
            percolateRequest.indices(Strings.splitStringByCommaToArray(request.param("percolate_index", index)));
            percolateRequest.documentType(request.param("percolate_type", type));
            GetRequest getRequest = new GetRequest(index, type, request.param("id"));
            getRequest.routing(request.param("routing"));
            getRequest.preference(request.param("preference"));
            getRequest.refresh(request.paramAsBoolean("refresh", getRequest.refresh()));
            getRequest.realtime(request.paramAsBoolean("realtime", null));
            getRequest.version(RestActions.parseVersion(request));
            getRequest.versionType(VersionType.fromString(request.param("version_type"), getRequest.versionType()));
            percolateRequest.getRequest(getRequest);
            percolateRequest.routing(request.param("percolate_routing"));
            percolateRequest.preference(request.param("percolate_preference"));
            percolateRequest.source(request.content(), request.contentUnsafe());
            percolateRequest.indicesOptions(IndicesOptions.fromRequest(request, percolateRequest.indicesOptions()));
            return percolateRequest;
        }
    }

    final static class RestCountPercolateExistingDocHandler extends RestPercolateExistingDocHandler {

        private RestCountPercolateExistingDocHandler(Settings settings, Client client) {
            super(settings, client);
        }

        @Override
        protected PercolateRequest newRequest(RestRequest request) {
            PercolateRequest percolateRequest = super.newRequest(request);
            percolateRequest.onlyCount(true);
            return percolateRequest;
        }
    }
}
