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

package org.elasticsearch.client;

import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.message.BasicHttpResponse;

import java.io.IOException;

/**
 * Simple {@link CloseableHttpResponse} impl needed to easily create http responses that are closeable given that
 * org.apache.http.impl.execchain.HttpResponseProxy is not public.
 */
class CloseableBasicHttpResponse extends BasicHttpResponse implements CloseableHttpResponse {

    public CloseableBasicHttpResponse(StatusLine statusline) {
        super(statusline);
    }

    @Override
    public void close() throws IOException {
        //nothing to close
    }
}