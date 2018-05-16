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

public class HttpPipelinedResponse<Response, Listener> implements Comparable<HttpPipelinedResponse<Response, Listener>> {

    private final int sequence;
    private final Response response;
    private final Listener listener;

    public HttpPipelinedResponse(int sequence, Response response, Listener listener) {
        this.sequence = sequence;
        this.response = response;
        this.listener = listener;
    }

    public int getSequence() {
        return sequence;
    }

    public Response getResponse() {
        return response;
    }

    public Listener getListener() {
        return listener;
    }

    @Override
    public int compareTo(HttpPipelinedResponse<Response, Listener> o) {
        return Integer.compare(sequence, o.sequence);
    }
}
