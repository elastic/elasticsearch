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

package org.elasticsearch.action;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportResponseHandler;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

/**
 * A generic proxy that will execute the given action against a specific node.
 */
public class TransportActionNodeProxy<Request extends ActionRequest, Response extends ActionResponse> extends AbstractComponent {

    protected final TransportService transportService;

    private final GenericAction<Request, Response> action;

    private final TransportRequestOptions transportOptions;

    @Inject
    public TransportActionNodeProxy(Settings settings, GenericAction<Request, Response> action, TransportService transportService) {
        super(settings);
        this.action = action;
        this.transportService = transportService;
        this.transportOptions = action.transportOptions(settings);
    }

    public void execute(DiscoveryNode node, final Request request, final ActionListener<Response> listener) {
        ActionRequestValidationException validationException = request.validate();
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }
        transportService.sendRequest(node, action.name(), request, transportOptions, new BaseTransportResponseHandler<Response>() {
            @Override
            public Response newInstance() {
                return action.newResponse();
            }

            @Override
            public String executor() {
                if (request.listenerThreaded()) {
                    return ThreadPool.Names.LISTENER;
                }
                return ThreadPool.Names.SAME;
            }

            @Override
            public void handleResponse(Response response) {
                listener.onResponse(response);
            }

            @Override
            public void handleException(TransportException exp) {
                listener.onFailure(exp);
            }
        });
    }

}
