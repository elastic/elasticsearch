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

package org.elasticsearch.memcached.netty;

import org.elasticsearch.common.netty.channel.ChannelHandlerContext;
import org.elasticsearch.common.netty.channel.MessageEvent;
import org.elasticsearch.common.netty.channel.SimpleChannelUpstreamHandler;
import org.elasticsearch.memcached.MemcachedRestRequest;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.StringRestResponse;

import static org.elasticsearch.rest.RestStatus.*;

/**
 * @author kimchy (shay.banon)
 */
public class MemcachedDispatcher extends SimpleChannelUpstreamHandler {

    public static final Object IGNORE_REQUEST = new Object();

    private final RestController restController;

    public MemcachedDispatcher(RestController restController) {
        this.restController = restController;
    }

    @Override public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        if (e.getMessage() == IGNORE_REQUEST) {
            super.messageReceived(ctx, e);
            return;
        }
        MemcachedRestRequest request = (MemcachedRestRequest) e.getMessage();
        MemcachedRestChannel channel = new MemcachedRestChannel(ctx.getChannel(), request);

        if (!restController.dispatchRequest(request, channel)) {
            channel.sendResponse(new StringRestResponse(BAD_REQUEST, "No handler found for uri [" + request.uri() + "] and method [" + request.method() + "]"));
        }

        super.messageReceived(ctx, e);
    }
}
