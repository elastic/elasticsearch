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

import org.apache.lucene.util.BytesRef;

/**
 * An http response that is built on top of {@link org.apache.lucene.util.BytesRef}.
 * <p/>
 * <p>Note, this class assumes that the utf8 result is not thread safe.
 */
public class Utf8RestResponse extends AbstractRestResponse implements RestResponse {

    public static final BytesRef EMPTY = new BytesRef();

    private final RestStatus status;

    private final BytesRef utf8Result;

    private final BytesRef prefixUtf8Result;

    private final BytesRef suffixUtf8Result;

    public Utf8RestResponse(RestStatus status) {
        this(status, EMPTY);
    }

    public Utf8RestResponse(RestStatus status, BytesRef utf8Result) {
        this(status, utf8Result, null, null);
    }

    public Utf8RestResponse(RestStatus status, BytesRef utf8Result,
                            BytesRef prefixUtf8Result, BytesRef suffixUtf8Result) {
        this.status = status;
        this.utf8Result = utf8Result;
        this.prefixUtf8Result = prefixUtf8Result;
        this.suffixUtf8Result = suffixUtf8Result;
    }

    @Override
    public boolean contentThreadSafe() {
        return true;
    }

    @Override
    public String contentType() {
        return "text/plain; charset=UTF-8";
    }

    @Override
    public byte[] content() {
        return utf8Result.bytes;
    }

    @Override
    public int contentLength() {
        return utf8Result.length;
    }

    @Override
    public int contentOffset() {
        return utf8Result.offset;
    }

    @Override
    public RestStatus status() {
        return status;
    }

    @Override
    public byte[] prefixContent() {
        return prefixUtf8Result != null ? prefixUtf8Result.bytes : null;
    }

    @Override
    public int prefixContentLength() {
        return prefixUtf8Result != null ? prefixUtf8Result.length : 0;
    }

    @Override
    public int prefixContentOffset() {
        return prefixUtf8Result != null ? prefixUtf8Result.offset : 0;
    }

    @Override
    public byte[] suffixContent() {
        return suffixUtf8Result != null ? suffixUtf8Result.bytes : null;
    }

    @Override
    public int suffixContentLength() {
        return suffixUtf8Result != null ? suffixUtf8Result.length : 0;
    }

    @Override
    public int suffixContentOffset() {
        return suffixUtf8Result != null ? suffixUtf8Result.offset : 0;
    }
}