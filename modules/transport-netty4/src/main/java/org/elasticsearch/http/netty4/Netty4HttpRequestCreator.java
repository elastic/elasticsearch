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

package org.elasticsearch.http.netty4;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.FullHttpRequest;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.http.HttpRequestMemoryController;

import java.util.List;

class Netty4HttpRequestCreator extends MessageToMessageDecoder<FullHttpRequest> {

    private final HttpRequestMemoryController memoryController;

    Netty4HttpRequestCreator(HttpRequestMemoryController memoryController) {
        this.memoryController = memoryController;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, FullHttpRequest msg, List<Object> out) throws Exception {
        Tuple<Releasable, CircuitBreakingException> breakerMetadata = memoryController.finish();
        final Netty4HttpRequest netty4HttpRequest;
        if (breakerMetadata.v2() != null) {
            netty4HttpRequest = new Netty4HttpRequest(msg, breakerMetadata.v1(), breakerMetadata.v2());
        } else {
            netty4HttpRequest = new Netty4HttpRequest(msg, breakerMetadata.v1());
        }
        out.add(netty4HttpRequest);
    }
}
