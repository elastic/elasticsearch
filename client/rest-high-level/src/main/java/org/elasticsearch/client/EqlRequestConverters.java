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

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.client.eql.EqlSearchRequest;
import org.elasticsearch.client.eql.EqlStatsRequest;

import java.io.IOException;

import static org.elasticsearch.client.RequestConverters.REQUEST_BODY_CONTENT_TYPE;
import static org.elasticsearch.client.RequestConverters.createEntity;

final class EqlRequestConverters {

    static Request search(EqlSearchRequest eqlSearchRequest) throws IOException {
        String endpoint = new RequestConverters.EndpointBuilder()
            .addCommaSeparatedPathParts(eqlSearchRequest.indices())
            .addPathPartAsIs("_eql", "search")
            .build();
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withIndicesOptions(eqlSearchRequest.indicesOptions());
        request.setEntity(createEntity(eqlSearchRequest, REQUEST_BODY_CONTENT_TYPE));
        request.addParameters(parameters.asMap());
        return request;
    }

    static Request stats(EqlStatsRequest eqlStatsRequest) throws IOException {
        String endpoint = new RequestConverters.EndpointBuilder()
            .addPathPartAsIs("_eql", "stats")
            .build();
        return new Request(HttpGet.METHOD_NAME, endpoint);
    }
}
