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

package org.elasticsearch.rest;

import org.elasticsearch.ElasticsearchException;

import java.io.Closeable;

/**
 * A filter allowing to filter rest operations.
 */
public abstract class RestFilter implements Closeable {

    /**
     * Optionally, the order of the filter. Execution is done from lowest value to highest.
     * It is a good practice to allow to configure this for the relevant filter.
     */
    public int order() {
        return 0;
    }

    @Override
    public void close() throws ElasticsearchException {
        // a no op
    }

    /**
     * Process the rest request. Using the channel to send a response, or the filter chain to continue
     * processing the request.
     */
    public abstract void process(RestRequest request, RestChannel channel, RestFilterChain filterChain) throws Exception;
}
