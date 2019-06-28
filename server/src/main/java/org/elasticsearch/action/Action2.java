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

package org.elasticsearch.action;


import org.elasticsearch.common.io.stream.Writeable;

/**
 * An action for which the response class implements {@link org.elasticsearch.common.io.stream.Writeable}.
 */
public class Action2<Response extends ActionResponse> extends Action<Response> {
    private final Writeable.Reader<Response> responseReader;

    public Action2(String name, Writeable.Reader<Response> responseReader) {
        super(name);
        this.responseReader = responseReader;
    }

    @Override
    public Response newResponse() {
        throw new UnsupportedOperationException();
    }

    /**
     * Get a reader that can create a new instance of the class from a {@link org.elasticsearch.common.io.stream.StreamInput}
     */
    public Writeable.Reader<Response> getResponseReader() {
        return responseReader;
    }
}
