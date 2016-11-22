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

package org.elasticsearch.client.advanced.get;

import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.Map;

/**
 * Get a document
 */
public class GetRestOperation {

    public static Response doExecute(RestClient client, GetRestRequest request) throws IOException {
        return client.performRequest("GET",
            "/" + request.getIndex() + "/" + request.getType() + "/" + request.getId());
    }

    public static void doExecute(RestClient client, GetRestRequest request, ResponseListener listener) throws IOException {
        client.performRequestAsync("GET",
            "/" + request.getIndex() + "/" + request.getType() + "/" + request.getId(), listener);
    }

    public static GetRestResponse toRestResponse(Map<String, Object> response) throws IOException {
        // Read from the map as we don't want to use reflection
        GetRestResponse restResponse = new GetRestResponse();
        boolean found = (boolean) response.get("found");
        restResponse.setFound(found);
        Map<String, Object> source = (Map<String, Object>) response.get("_source");
        restResponse.setSource(source);
        return restResponse;
    }
}
