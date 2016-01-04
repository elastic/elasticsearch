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

import org.elasticsearch.action.percolate.MultiPercolateRequest;
import org.elasticsearch.action.percolate.MultiPercolateResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseStandardRegistrationsRestHandler;
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
public class RestMultiPercolateAction extends BaseStandardRegistrationsRestHandler {

    private final boolean allowExplicitIndex;

    public RestMultiPercolateAction(RestGlobalContext context) {
        super(context, new Method[] {POST, GET}, "/_mpercolate", "/{index}/_mpercolate", "/{index}/{type}/_mpercolate");
        this.allowExplicitIndex = settings.getAsBoolean("rest.action.multi.allow_explicit_index", true);
    }

    @Override
    public void handleRequest(final RestRequest restRequest, final RestChannel restChannel, final Client client) throws Exception {
        MultiPercolateRequest multiPercolateRequest = new MultiPercolateRequest();
        multiPercolateRequest.indicesOptions(IndicesOptions.fromRequest(restRequest, multiPercolateRequest.indicesOptions()));
        multiPercolateRequest.indices(Strings.splitStringByCommaToArray(restRequest.param("index")));
        multiPercolateRequest.documentType(restRequest.param("type"));
        multiPercolateRequest.add(RestActions.getRestContent(restRequest), allowExplicitIndex);

        client.multiPercolate(multiPercolateRequest, new RestToXContentListener<MultiPercolateResponse>(restChannel));
    }

}
