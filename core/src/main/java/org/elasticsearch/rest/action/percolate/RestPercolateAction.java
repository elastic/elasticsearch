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
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.BaseMultiMethodRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestGlobalContext;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestToXContentListener;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 *
 */
public class RestPercolateAction extends BaseMultiMethodRestHandler {
    public RestPercolateAction(RestGlobalContext context) {
        super(context, new Method[] {GET, POST}, "/{index}/{type}/_percolate");
    }

    static void parseDocPercolate(PercolateRequest percolateRequest, RestRequest restRequest, RestChannel restChannel, final Client client) {
        percolateRequest.indices(Strings.splitStringByCommaToArray(restRequest.param("index")));
        percolateRequest.documentType(restRequest.param("type"));
        percolateRequest.routing(restRequest.param("routing"));
        percolateRequest.preference(restRequest.param("preference"));
        percolateRequest.source(RestActions.getRestContent(restRequest));

        percolateRequest.indicesOptions(IndicesOptions.fromRequest(restRequest, percolateRequest.indicesOptions()));
        executePercolate(percolateRequest, restChannel, client);
    }

    static void parseExistingDocPercolate(PercolateRequest percolateRequest, RestRequest restRequest, RestChannel restChannel, final Client client) {
        String index = restRequest.param("index");
        String type = restRequest.param("type");
        percolateRequest.indices(Strings.splitStringByCommaToArray(restRequest.param("percolate_index", index)));
        percolateRequest.documentType(restRequest.param("percolate_type", type));

        GetRequest getRequest = new GetRequest(index, type,
                restRequest.param("id"));
        getRequest.routing(restRequest.param("routing"));
        getRequest.preference(restRequest.param("preference"));
        getRequest.refresh(restRequest.paramAsBoolean("refresh", getRequest.refresh()));
        getRequest.realtime(restRequest.paramAsBoolean("realtime", null));
        getRequest.version(RestActions.parseVersion(restRequest));
        getRequest.versionType(VersionType.fromString(restRequest.param("version_type"), getRequest.versionType()));

        percolateRequest.getRequest(getRequest);
        percolateRequest.routing(restRequest.param("percolate_routing"));
        percolateRequest.preference(restRequest.param("percolate_preference"));
        percolateRequest.source(RestActions.getRestContent(restRequest));

        percolateRequest.indicesOptions(IndicesOptions.fromRequest(restRequest, percolateRequest.indicesOptions()));
        executePercolate(percolateRequest, restChannel, client);
    }

    static void executePercolate(final PercolateRequest percolateRequest, final RestChannel restChannel, final Client client) {
        client.percolate(percolateRequest, new RestToXContentListener<PercolateResponse>(restChannel));
    }

    @Override
    public void handleRequest(RestRequest restRequest, RestChannel restChannel, final Client client) {
        PercolateRequest percolateRequest = new PercolateRequest();
        parseDocPercolate(percolateRequest, restRequest, restChannel, client);
    }

    public static final class RestCountPercolateDocHandler extends BaseMultiMethodRestHandler {

        public RestCountPercolateDocHandler(RestGlobalContext context) {
            super(context, new Method[] {GET, POST}, "/{index}/{type}/_percolate/count");
        }

        @Override
        public void handleRequest(RestRequest restRequest, RestChannel restChannel, final Client client) {
            PercolateRequest percolateRequest = new PercolateRequest();
            percolateRequest.onlyCount(true);
            parseDocPercolate(percolateRequest, restRequest, restChannel, client);
        }
    }

    public static final class RestPercolateExistingDocHandler extends BaseMultiMethodRestHandler {

        public RestPercolateExistingDocHandler(RestGlobalContext context) {
            super(context, new Method[] {GET, POST}, "/{index}/{type}/{id}/_percolate");
        }

        @Override
        public void handleRequest(RestRequest restRequest, RestChannel restChannel, final Client client) {
            PercolateRequest percolateRequest = new PercolateRequest();
            parseExistingDocPercolate(percolateRequest, restRequest, restChannel, client);
        }
    }

    public static final class RestCountPercolateExistingDocHandler extends BaseMultiMethodRestHandler {

        public RestCountPercolateExistingDocHandler(RestGlobalContext context) {
            super(context, new Method[] {GET, POST}, "/{index}/{type}/{id}/_percolate/count");
        }

        @Override
        public void handleRequest(RestRequest restRequest, RestChannel restChannel, final Client client) {
            PercolateRequest percolateRequest = new PercolateRequest();
            percolateRequest.onlyCount(true);
            parseExistingDocPercolate(percolateRequest, restRequest, restChannel, client);
        }
    }
}
