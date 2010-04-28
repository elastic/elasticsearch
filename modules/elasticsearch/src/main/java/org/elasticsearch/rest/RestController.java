/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.util.component.AbstractLifecycleComponent;
import org.elasticsearch.util.guice.inject.Inject;
import org.elasticsearch.util.path.PathTrie;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;

import static org.elasticsearch.rest.RestResponse.Status.*;

/**
 * @author kimchy (Shay Banon)
 */
public class RestController extends AbstractLifecycleComponent<RestController> {

    private final PathTrie<RestHandler> getHandlers = new PathTrie<RestHandler>();
    private final PathTrie<RestHandler> postHandlers = new PathTrie<RestHandler>();
    private final PathTrie<RestHandler> putHandlers = new PathTrie<RestHandler>();
    private final PathTrie<RestHandler> deleteHandlers = new PathTrie<RestHandler>();

    @Inject public RestController(Settings settings) {
        super(settings);
    }

    @Override protected void doStart() throws ElasticSearchException {
    }

    @Override protected void doStop() throws ElasticSearchException {
    }

    @Override protected void doClose() throws ElasticSearchException {
    }

    public void registerHandler(RestRequest.Method method, String path, RestHandler handler) {
        switch (method) {
            case GET:
                getHandlers.insert(path, handler);
                break;
            case DELETE:
                deleteHandlers.insert(path, handler);
                break;
            case POST:
                postHandlers.insert(path, handler);
                break;
            case PUT:
                putHandlers.insert(path, handler);
                break;
            default:
                throw new ElasticSearchIllegalArgumentException("Can't handle [" + method + "] for path [" + path + "]");
        }
    }

    public void dispatchRequest(final RestRequest request, final RestChannel channel) {
        final RestHandler handler = getHandler(request);
        if (handler == null) {
            channel.sendResponse(new StringRestResponse(BAD_REQUEST, "No handler found for uri [" + request.uri() + "] and method [" + request.method() + "]"));
            return;
        }
        try {
            handler.handleRequest(request, channel);
        } catch (Exception e) {
            try {
                channel.sendResponse(new JsonThrowableRestResponse(request, e));
            } catch (IOException e1) {
                logger.error("Failed to send failure response for uri [" + request.uri() + "]", e1);
            }
        }
    }

    private RestHandler getHandler(RestRequest request) {
        String path = getPath(request);
        RestRequest.Method method = request.method();
        if (method == RestRequest.Method.GET) {
            return getHandlers.retrieve(path, request.params());
        } else if (method == RestRequest.Method.POST) {
            return postHandlers.retrieve(path, request.params());
        } else if (method == RestRequest.Method.PUT) {
            return putHandlers.retrieve(path, request.params());
        } else if (method == RestRequest.Method.DELETE) {
            return deleteHandlers.retrieve(path, request.params());
        } else {
            return null;
        }
    }

    private String getPath(RestRequest request) {
        return request.path();
    }
}
