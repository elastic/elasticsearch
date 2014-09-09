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

import org.elasticsearch.client.Client;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;

/**
 * Base handler for REST requests.
 *
 * This handler makes sure that the headers & context of the handled {@link RestRequest requests} are copied over to
 * the transport requests executed by the associated client. While the context is fully copied over, not all the headers
 * are copied, but a selected few. It is possible to control what headers are copied over by registering them using
 * {@link RestClientFactory#addRelevantHeaders(String...)}
 */
public abstract class BaseRestHandler extends AbstractComponent implements RestHandler {

    private final RestClientFactory restClientFactory;

    protected BaseRestHandler(Settings settings, RestClientFactory restClientFactory) {
        super(settings);
        this.restClientFactory = restClientFactory;
    }

    @Override
    public final void handleRequest(RestRequest request, RestChannel channel) throws Exception {
        handleRequest(request, channel, restClientFactory.client(request));
    }

    protected abstract void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception;
}