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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import org.elasticsearch.http.HttpPipelinedRequest;
import org.elasticsearch.test.ESTestCase;

public class Netty4HttpRequestHandlerTests extends ESTestCase {
    public void testExtractHttpRequestWithUnpooledContent() {
        HttpPipelinedRequest<FullHttpRequest> pipelinedRequest = createPipelinedRequest(false);
        FullHttpRequest extractedRequest = Netty4HttpRequestHandler.httpRequest(pipelinedRequest);
        assertSame(extractedRequest, pipelinedRequest.getRequest());
    }

    public void testExtractHttpRequestWithPooledContent() {
        HttpPipelinedRequest<FullHttpRequest> pipelinedRequest = createPipelinedRequest(true);
        assertEquals(1, pipelinedRequest.getRequest().refCnt());
        FullHttpRequest extractedRequest = Netty4HttpRequestHandler.httpRequest(pipelinedRequest);
        assertNotSame(extractedRequest, pipelinedRequest.getRequest());
        assertEquals(0, pipelinedRequest.getRequest().refCnt());
    }

    private HttpPipelinedRequest<FullHttpRequest> createPipelinedRequest(boolean usePooledAllocator) {
        ByteBufAllocator alloc = usePooledAllocator ? PooledByteBufAllocator.DEFAULT : UnpooledByteBufAllocator.DEFAULT;
        ByteBuf content = alloc.buffer(5);
        ByteBufUtil.writeAscii(content, randomAlphaOfLength(content.capacity()));
        final FullHttpRequest nettyRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/", content);
        HttpUtil.setContentLength(nettyRequest, content.capacity());
        return new HttpPipelinedRequest<>(0, nettyRequest);
    }
}
