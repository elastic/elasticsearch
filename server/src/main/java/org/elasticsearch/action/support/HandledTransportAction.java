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
package org.elasticsearch.action.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.util.function.Supplier;

/**
 * A TransportAction that self registers a handler into the transport service
 */
public abstract class HandledTransportAction<Request extends ActionRequest, Response extends ActionResponse>
        extends TransportAction<Request, Response> {
    protected HandledTransportAction(String actionName, TransportService transportService,
                                     ActionFilters actionFilters, Supplier<Request> request) {
        this(actionName, true, transportService, actionFilters, request);
    }

    protected HandledTransportAction(String actionName, TransportService transportService,
                                     ActionFilters actionFilters, Writeable.Reader<Request> requestReader) {
        this(actionName, true, transportService, actionFilters, requestReader);
    }

    protected HandledTransportAction(String actionName, boolean canTripCircuitBreaker,
                                     TransportService transportService, ActionFilters actionFilters, Supplier<Request> request) {
        super(actionName, actionFilters, transportService.getTaskManager());
        transportService.registerRequestHandler(actionName, request, ThreadPool.Names.SAME, false, canTripCircuitBreaker,
            new TransportHandler());
    }

    protected HandledTransportAction(String actionName, boolean canTripCircuitBreaker,
                                     TransportService transportService, ActionFilters actionFilters,
                                     Writeable.Reader<Request> requestReader) {
        super(actionName, actionFilters, transportService.getTaskManager());
        transportService.registerRequestHandler(actionName, ThreadPool.Names.SAME, false, canTripCircuitBreaker, requestReader,
            new TransportHandler());
    }

    class TransportHandler implements TransportRequestHandler<Request> {
        @Override
        public final void messageReceived(final Request request, final TransportChannel channel, Task task) throws Exception {
            // We already got the task created on the network layer - no need to create it again on the transport layer
            Logger logger = HandledTransportAction.this.logger;
            execute(task, request, new ChannelActionListener<>(channel, actionName, request));
        }
    }

    public static final class ChannelActionListener<Response extends TransportResponse, Request extends TransportRequest> implements
        ActionListener<Response> {
        private final Logger logger = LogManager.getLogger(getClass());
        private final TransportChannel channel;
        private final Request request;
        private final String actionName;

        public ChannelActionListener(TransportChannel channel, String actionName, Request request) {
            this.channel = channel;
            this.request = request;
            this.actionName = actionName;
        }

        @Override
        public void onResponse(Response response) {
            try {
                channel.sendResponse(response);
            } catch (Exception e) {
                onFailure(e);
            }
        }

        @Override
        public void onFailure(Exception e) {
            try {
                channel.sendResponse(e);
            } catch (Exception e1) {
                logger.warn(() -> new ParameterizedMessage(
                    "Failed to send error response for action [{}] and request [{}]", actionName, request), e1);
            }
        }
    }

}
