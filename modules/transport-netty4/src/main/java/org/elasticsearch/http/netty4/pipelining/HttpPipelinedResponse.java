package org.elasticsearch.http.netty4.pipelining;

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

import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.util.ReferenceCounted;

class HttpPipelinedResponse implements Comparable<HttpPipelinedResponse>, ReferenceCounted {

    private final FullHttpResponse response;
    private final ChannelPromise promise;
    private final int sequence;

    HttpPipelinedResponse(FullHttpResponse response, ChannelPromise promise, int sequence) {
        this.response = response;
        this.promise = promise;
        this.sequence = sequence;
    }

    public FullHttpResponse response() {
        return response;
    }

    public ChannelPromise promise() {
        return promise;
    }

    public int sequence() {
        return sequence;
    }

    @Override
    public int compareTo(HttpPipelinedResponse o) {
        return Integer.compare(sequence, o.sequence);
    }

    @Override
    public int refCnt() {
        return response.refCnt();
    }

    @Override
    public ReferenceCounted retain() {
        response.retain();
        return this;
    }

    @Override
    public ReferenceCounted retain(int increment) {
        response.retain(increment);
        return this;
    }

    @Override
    public ReferenceCounted touch() {
        response.touch();
        return this;
    }

    @Override
    public ReferenceCounted touch(Object hint) {
        response.touch(hint);
        return this;
    }

    @Override
    public boolean release() {
        return response.release();
    }

    @Override
    public boolean release(int decrement) {
        return response.release(decrement);
    }

}
