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

package org.elasticsearch.transport;


import java.lang.reflect.Constructor;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

/**
 *
 */
public class RequestHandlerRegistry<Request extends TransportRequest> {

    private final String action;
    private final TransportRequestHandler<Request> handler;
    private final boolean forceExecution;
    private final String executor;
    private final Supplier<Request> requestFactory;

    public RequestHandlerRegistry(String action, Supplier<Request> requestFactory, TransportRequestHandler<Request> handler, String executor, boolean forceExecution) {
        this.action = action;
        this.requestFactory = requestFactory;
        assert newRequest() != null;
        this.handler = handler;
        this.forceExecution = forceExecution;
        this.executor = executor;
    }

    public String getAction() {
        return action;
    }

    public Request newRequest() {
            return requestFactory.get();
    }

    public TransportRequestHandler<Request> getHandler() {
        return handler;
    }

    public boolean isForceExecution() {
        return forceExecution;
    }

    public String getExecutor() {
        return executor;
    }
}
