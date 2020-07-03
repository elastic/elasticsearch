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
package org.elasticsearch.http;

public class HttpPipelinedResponse implements HttpPipelinedMessage, HttpResponse {

    private final int sequence;
    private final HttpResponse delegate;

    public HttpPipelinedResponse(int sequence, HttpResponse delegate) {
        this.sequence = sequence;
        this.delegate = delegate;
    }

    @Override
    public int getSequence() {
        return sequence;
    }

    @Override
    public void addHeader(String name, String value) {
        delegate.addHeader(name, value);
    }

    @Override
    public boolean containsHeader(String name) {
        return delegate.containsHeader(name);
    }

    public HttpResponse getDelegateRequest() {
        return delegate;
    }
}
