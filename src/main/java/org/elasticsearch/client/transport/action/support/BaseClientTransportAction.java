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

package org.elasticsearch.client.transport.action.support;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.transport.action.ClientTransportAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportResponseHandler;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.lang.reflect.Constructor;

import static org.elasticsearch.action.support.PlainActionFuture.*;

/**
 * @author kimchy (shay.banon)
 */
public abstract class BaseClientTransportAction<Request extends ActionRequest, Response extends ActionResponse> extends AbstractComponent implements ClientTransportAction<Request, Response> {

    protected final TransportService transportService;

    private final Constructor<Response> responseConstructor;

    protected BaseClientTransportAction(Settings settings, TransportService transportService, Class<Response> type) {
        super(settings);
        this.transportService = transportService;
        try {
            this.responseConstructor = type.getDeclaredConstructor();
        } catch (NoSuchMethodException e) {
            throw new ElasticSearchIllegalArgumentException("No default constructor is declared for [" + type.getName() + "]");
        }
        responseConstructor.setAccessible(true);
    }

    @Override public ActionFuture<Response> execute(DiscoveryNode node, Request request) throws ElasticSearchException {
        PlainActionFuture<Response> future = newFuture();
        request.listenerThreaded(false);
        execute(node, request, future);
        return future;
    }

    @Override public void execute(DiscoveryNode node, final Request request, final ActionListener<Response> listener) {
        transportService.sendRequest(node, action(), request, options(), new BaseTransportResponseHandler<Response>() {
            @Override public Response newInstance() {
                return BaseClientTransportAction.this.newInstance();
            }

            @Override public String executor() {
                if (request.listenerThreaded()) {
                    return ThreadPool.Names.CACHED;
                }
                return ThreadPool.Names.SAME;
            }

            @Override public void handleResponse(Response response) {
                listener.onResponse(response);
            }

            @Override public void handleException(TransportException exp) {
                listener.onFailure(exp);
            }
        });
    }

    protected TransportRequestOptions options() {
        return TransportRequestOptions.EMPTY;
    }

    protected abstract String action();

    protected Response newInstance() {
        try {
            return responseConstructor.newInstance();
        } catch (Exception e) {
            throw new ElasticSearchIllegalStateException("Failed to create a new instance");
        }
    }
}
