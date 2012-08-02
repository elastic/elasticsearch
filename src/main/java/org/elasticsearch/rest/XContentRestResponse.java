/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.rest;

import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.common.util.concurrent.ThreadLocals;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 *
 */
public class XContentRestResponse extends AbstractRestResponse {

    private static final byte[] END_JSONP;

    static {
        UnicodeUtil.UTF8Result U_END_JSONP = new UnicodeUtil.UTF8Result();
        UnicodeUtil.UTF16toUTF8(");", 0, ");".length(), U_END_JSONP);
        END_JSONP = new byte[U_END_JSONP.length];
        System.arraycopy(U_END_JSONP.result, 0, END_JSONP, 0, U_END_JSONP.length);
    }

    private static ThreadLocal<ThreadLocals.CleanableValue<UnicodeUtil.UTF8Result>> prefixCache = new ThreadLocal<ThreadLocals.CleanableValue<UnicodeUtil.UTF8Result>>() {
        @Override
        protected ThreadLocals.CleanableValue<UnicodeUtil.UTF8Result> initialValue() {
            return new ThreadLocals.CleanableValue<UnicodeUtil.UTF8Result>(new UnicodeUtil.UTF8Result());
        }
    };

    private final UnicodeUtil.UTF8Result prefixUtf8Result;

    private final RestStatus status;

    private final XContentBuilder builder;

    public XContentRestResponse(RestRequest request, RestStatus status, XContentBuilder builder) throws IOException {
        this.builder = builder;
        this.status = status;
        this.prefixUtf8Result = startJsonp(request);
    }

    public XContentBuilder builder() {
        return this.builder;
    }

    @Override
    public String contentType() {
        return builder.contentType().restContentType();
    }

    @Override
    public boolean contentThreadSafe() {
        return false;
    }

    @Override
    public byte[] content() throws IOException {
        return builder.bytes().array();
    }

    @Override
    public int contentLength() throws IOException {
        return builder.bytes().length();
    }

    @Override
    public RestStatus status() {
        return this.status;
    }

    @Override
    public byte[] prefixContent() {
        if (prefixUtf8Result != null) {
            return prefixUtf8Result.result;
        }
        return null;
    }

    @Override
    public int prefixContentLength() {
        if (prefixUtf8Result != null) {
            return prefixUtf8Result.length;
        }
        return 0;
    }

    @Override
    public byte[] suffixContent() {
        if (prefixUtf8Result != null) {
            return END_JSONP;
        }
        return null;
    }

    @Override
    public int suffixContentLength() {
        if (prefixUtf8Result != null) {
            return END_JSONP.length;
        }
        return 0;
    }

    private static UnicodeUtil.UTF8Result startJsonp(RestRequest request) {
        String callback = request.param("callback");
        if (callback == null) {
            return null;
        }
        UnicodeUtil.UTF8Result result = prefixCache.get().get();
        UnicodeUtil.UTF16toUTF8(callback, 0, callback.length(), result);
        result.result[result.length] = '(';
        result.length++;
        return result;
    }
}