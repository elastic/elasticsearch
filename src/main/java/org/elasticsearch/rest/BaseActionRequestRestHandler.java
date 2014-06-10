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

package org.elasticsearch.rest;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;

/**
 * Base handlers for {@link org.elasticsearch.rest.RestRequest}s that map to an {@link org.elasticsearch.action.ActionRequest}s
 */
public abstract class BaseActionRequestRestHandler<Request extends ActionRequest<Request>> extends BaseRestHandler {

    protected BaseActionRequestRestHandler(Settings settings, Client client) {
        super(settings, client);
    }

    @Override
    public final void handleRequest(RestRequest restRequest, RestChannel channel) throws Exception {
        Request actionRequest = newRequest(restRequest);
        doHandleRequest(restRequest, channel, copyHeaders(restRequest, actionRequest));
    }

    /**
     * Creates a new {@link org.elasticsearch.action.ActionRequest} based on the current {@link org.elasticsearch.rest.RestRequest}
     */
    protected abstract Request newRequest(RestRequest request) throws Exception;

    /**
     * Executes the request
     */
    protected abstract void doHandleRequest(RestRequest restRequest, RestChannel restChannel, Request request);
}