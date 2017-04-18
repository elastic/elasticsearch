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

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.usage.UsageService;

import java.util.function.UnaryOperator;

public class UsageRestHandlerWrapper implements RestHandler {

    private RestHandler delegate;
    private UsageService usageService;
    private Class<? extends RestHandler> handlerClass;

    public UsageRestHandlerWrapper(RestHandler delegate, UsageService usageService, Class<? extends RestHandler> handlerClass) {
        this.delegate = delegate;
        this.usageService = usageService;
        this.handlerClass = handlerClass;
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
        usageService.addRestCall(handlerClass.getName());
        delegate.handleRequest(request, channel, client);
    }

    public static UnaryOperator<RestHandler> wrapRestHandlerOperator(UnaryOperator<RestHandler> operator, UsageService usageService) {
        final UnaryOperator<RestHandler> resolvedOperator;
        if (operator == null) {
            resolvedOperator = h -> h;
        } else {
            resolvedOperator = operator;
        }
        return (handler) -> new UsageRestHandlerWrapper(resolvedOperator.apply(handler), usageService, handler.getClass());
    }

}
