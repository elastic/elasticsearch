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
package org.elasticsearch.legacy.rest.action.percolate;

import org.elasticsearch.legacy.action.percolate.MultiPercolateRequest;
import org.elasticsearch.legacy.action.percolate.MultiPercolateResponse;
import org.elasticsearch.legacy.action.support.IndicesOptions;
import org.elasticsearch.legacy.client.Client;
import org.elasticsearch.legacy.common.Strings;
import org.elasticsearch.legacy.common.inject.Inject;
import org.elasticsearch.legacy.common.settings.Settings;
import org.elasticsearch.legacy.rest.*;
import org.elasticsearch.legacy.rest.action.support.RestActions;
import org.elasticsearch.legacy.rest.action.support.RestToXContentListener;

import static org.elasticsearch.legacy.rest.RestRequest.Method.GET;
import static org.elasticsearch.legacy.rest.RestRequest.Method.POST;

/**
 *
 */
public class RestMultiPercolateAction extends BaseRestHandler {

    private final boolean allowExplicitIndex;

    @Inject
    public RestMultiPercolateAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(POST, "/_mpercolate", this);
        controller.registerHandler(POST, "/{index}/_mpercolate", this);
        controller.registerHandler(POST, "/{index}/{type}/_mpercolate", this);

        controller.registerHandler(GET, "/_mpercolate", this);
        controller.registerHandler(GET, "/{index}/_mpercolate", this);
        controller.registerHandler(GET, "/{index}/{type}/_mpercolate", this);

        this.allowExplicitIndex = settings.getAsBoolean("rest.action.multi.allow_explicit_index", true);
    }

    @Override
    public void handleRequest(final RestRequest restRequest, final RestChannel restChannel, final Client client) throws Exception {
        MultiPercolateRequest multiPercolateRequest = new MultiPercolateRequest();
        multiPercolateRequest.indicesOptions(IndicesOptions.fromRequest(restRequest, multiPercolateRequest.indicesOptions()));
        multiPercolateRequest.indices(Strings.splitStringByCommaToArray(restRequest.param("index")));
        multiPercolateRequest.documentType(restRequest.param("type"));
        multiPercolateRequest.add(RestActions.getRestContent(restRequest), restRequest.contentUnsafe(), allowExplicitIndex);

        client.multiPercolate(multiPercolateRequest, new RestToXContentListener<MultiPercolateResponse>(restChannel));
    }

}
