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

package org.elasticsearch.http.netty4.pipelining;

import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCounted;

/**
 * Permits downstream channel events to be ordered and signalled as to whether more are to come for
 * a given sequence.
 */
public class HttpPipelinedRequest implements ReferenceCounted {

    private final LastHttpContent last;
    private final int sequence;

    public HttpPipelinedRequest(final LastHttpContent last, final int sequence) {
        this.last = last;
        this.sequence = sequence;
    }

    public LastHttpContent last() {
        return last;
    }

    public HttpPipelinedResponse createHttpResponse(final FullHttpResponse response, final ChannelPromise promise) {
        return new HttpPipelinedResponse(response, promise, sequence);
    }

    @Override
    public int refCnt() {
        return last.refCnt();
    }

    @Override
    public ReferenceCounted retain() {
        last.retain();
        return this;
    }

    @Override
    public ReferenceCounted retain(int increment) {
        last.retain(increment);
        return this;
    }

    @Override
    public ReferenceCounted touch() {
        last.touch();
        return this;
    }

    @Override
    public ReferenceCounted touch(Object hint) {
        last.touch(hint);
        return this;
    }

    @Override
    public boolean release() {
        return last.release();
    }

    @Override
    public boolean release(int decrement) {
        return last.release(decrement);
    }

}
