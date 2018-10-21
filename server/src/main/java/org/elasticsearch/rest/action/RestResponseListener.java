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

package org.elasticsearch.rest.action;

import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;

/**
 * A REST enabled action listener that has a basic onFailure implementation, and requires
 * sub classes to only implement {@link #buildResponse(Object)}.
 */
public abstract class RestResponseListener<Response> extends RestActionListener<Response> {

    protected RestResponseListener(RestChannel channel) {
        super(channel);
    }

    @Override
    protected final void processResponse(Response response) throws Exception {
        channel.sendResponse(buildResponse(response));
    }

    /**
     * Builds the response to send back through the channel.
     */
    public abstract RestResponse buildResponse(Response response) throws Exception;
}
