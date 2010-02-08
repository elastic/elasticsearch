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

package org.elasticsearch.http.netty;

import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.http.HttpRequest;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author kimchy (Shay Banon)
 */
public class NettyHttpRequest implements HttpRequest {

    private final org.jboss.netty.handler.codec.http.HttpRequest request;

    private QueryStringDecoder queryStringDecoder;

    private static ThreadLocal<UnicodeUtil.UTF16Result> utf16Result = new ThreadLocal<UnicodeUtil.UTF16Result>() {
        @Override protected UnicodeUtil.UTF16Result initialValue() {
            return new UnicodeUtil.UTF16Result();
        }
    };

    public NettyHttpRequest(org.jboss.netty.handler.codec.http.HttpRequest request) {
        this.request = request;
        this.queryStringDecoder = new QueryStringDecoder(request.getUri());
    }

    @Override public Method method() {
        HttpMethod httpMethod = request.getMethod();
        if (httpMethod == HttpMethod.GET)
            return Method.GET;

        if (httpMethod == HttpMethod.POST)
            return Method.POST;

        if (httpMethod == HttpMethod.PUT)
            return Method.PUT;

        if (httpMethod == HttpMethod.DELETE)
            return Method.DELETE;

        return Method.GET;
    }

    @Override public String uri() {
        return request.getUri();
    }

    @Override public boolean hasContent() {
        return request.getContent().readableBytes() > 0;
    }

    @Override public String contentAsString() {
        UnicodeUtil.UTF16Result result = utf16Result.get();
        ChannelBuffer content = request.getContent();
        UTF8toUTF16(content, content.readerIndex(), content.readableBytes(), result);
        return new String(result.result, 0, result.length);
    }

    @Override public Set<String> headerNames() {
        return request.getHeaderNames();
    }

    @Override public String header(String name) {
        return request.getHeader(name);
    }

    @Override public List<String> headers(String name) {
        return request.getHeaders(name);
    }

    @Override public String cookie() {
        return request.getHeader(HttpHeaders.Names.COOKIE);
    }

    @Override public String param(String key) {
        List<String> keyParams = params(key);
        if (keyParams == null || keyParams.isEmpty()) {
            return null;
        }
        return keyParams.get(0);
    }

    @Override public List<String> params(String key) {
        return queryStringDecoder.getParameters().get(key);
    }

    @Override public Map<String, List<String>> params() {
        return queryStringDecoder.getParameters();
    }

    // LUCENE TRACK
    // The idea here is not to allocate all these byte arrays / char arrays again, just use the channel buffer to convert
    // directly into UTF16 from bytes that represent UTF8 ChannelBuffer

    public static void UTF8toUTF16(ChannelBuffer cb, final int offset, final int length, final UnicodeUtil.UTF16Result result) {

        final int end = offset + length;
        char[] out = result.result;
        if (result.offsets.length <= end) {
            int[] newOffsets = new int[2 * end];
            System.arraycopy(result.offsets, 0, newOffsets, 0, result.offsets.length);
            result.offsets = newOffsets;
        }
        final int[] offsets = result.offsets;

        // If incremental decoding fell in the middle of a
        // single unicode character, rollback to its start:
        int upto = offset;
        while (offsets[upto] == -1)
            upto--;

        int outUpto = offsets[upto];

        // Pre-allocate for worst case 1-for-1
        if (outUpto + length >= out.length) {
            char[] newOut = new char[2 * (outUpto + length)];
            System.arraycopy(out, 0, newOut, 0, outUpto);
            result.result = out = newOut;
        }

        while (upto < end) {

            final int b = cb.getByte(upto) & 0xff;
            final int ch;

            offsets[upto++] = outUpto;

            if (b < 0xc0) {
                assert b < 0x80;
                ch = b;
            } else if (b < 0xe0) {
                ch = ((b & 0x1f) << 6) + (cb.getByte(upto) & 0x3f);
                offsets[upto++] = -1;
            } else if (b < 0xf0) {
                ch = ((b & 0xf) << 12) + ((cb.getByte(upto) & 0x3f) << 6) + (cb.getByte(upto + 1) & 0x3f);
                offsets[upto++] = -1;
                offsets[upto++] = -1;
            } else {
                assert b < 0xf8;
                ch = ((b & 0x7) << 18) + ((cb.getByte(upto) & 0x3f) << 12) + ((cb.getByte(upto + 1) & 0x3f) << 6) + (cb.getByte(upto + 2) & 0x3f);
                offsets[upto++] = -1;
                offsets[upto++] = -1;
                offsets[upto++] = -1;
            }

            if (ch <= UNI_MAX_BMP) {
                // target is a character <= 0xFFFF
                out[outUpto++] = (char) ch;
            } else {
                // target is a character in range 0xFFFF - 0x10FFFF
                final int chHalf = ch - HALF_BASE;
                out[outUpto++] = (char) ((chHalf >> HALF_SHIFT) + UnicodeUtil.UNI_SUR_HIGH_START);
                out[outUpto++] = (char) ((chHalf & HALF_MASK) + UnicodeUtil.UNI_SUR_LOW_START);
            }
        }

        offsets[upto] = outUpto;
        result.length = outUpto;
    }

    private static final long UNI_MAX_BMP = 0x0000FFFF;

    private static final int HALF_BASE = 0x0010000;
    private static final long HALF_SHIFT = 10;
    private static final long HALF_MASK = 0x3FFL;
}
