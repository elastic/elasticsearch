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

package org.elasticsearch.http;

import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.util.json.JsonBuilder;

import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public class JsonHttpResponse extends Utf8HttpResponse {

    private static ThreadLocal<UnicodeUtil.UTF8Result> cache = new ThreadLocal<UnicodeUtil.UTF8Result>() {
        @Override protected UnicodeUtil.UTF8Result initialValue() {
            return new UnicodeUtil.UTF8Result();
        }
    };

    private static final UnicodeUtil.UTF8Result END_JSONP = new UnicodeUtil.UTF8Result();

    static {
        UnicodeUtil.UTF16toUTF8(");", 0, ");".length(), END_JSONP);
    }

    private static ThreadLocal<UnicodeUtil.UTF8Result> prefixCache = new ThreadLocal<UnicodeUtil.UTF8Result>() {
        @Override protected UnicodeUtil.UTF8Result initialValue() {
            return new UnicodeUtil.UTF8Result();
        }
    };

    public JsonHttpResponse(HttpRequest request, Status status) {
        super(status, EMPTY, startJsonp(request), endJsonp(request));
    }

    public JsonHttpResponse(HttpRequest request, Status status, JsonBuilder jsonBuilder) throws IOException {
        super(status, jsonBuilder.utf8(), startJsonp(request), endJsonp(request));
    }

    public JsonHttpResponse(HttpRequest request, Status status, String source) throws IOException {
        super(status, convert(source), startJsonp(request), endJsonp(request));
    }

    @Override public String contentType() {
        return "application/json; charset=UTF-8";
    }

    private static UnicodeUtil.UTF8Result convert(String content) {
        UnicodeUtil.UTF8Result result = cache.get();
        UnicodeUtil.UTF16toUTF8(content, 0, content.length(), result);
        return result;
    }

    private static UnicodeUtil.UTF8Result startJsonp(HttpRequest request) {
        String callback = request.param("callback");
        if (callback == null) {
            return null;
        }
        UnicodeUtil.UTF8Result result = prefixCache.get();
        UnicodeUtil.UTF16toUTF8(callback, 0, callback.length(), result);
        result.result[result.length] = '(';
        result.length++;
        return result;
    }

    private static UnicodeUtil.UTF8Result endJsonp(HttpRequest request) {
        String callback = request.param("callback");
        if (callback == null) {
            return null;
        }
        return END_JSONP;
    }

}
