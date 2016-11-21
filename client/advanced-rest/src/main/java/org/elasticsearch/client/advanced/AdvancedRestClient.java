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

package org.elasticsearch.client.advanced;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.advanced.delete.DeleteRestOperation;
import org.elasticsearch.client.advanced.delete.DeleteRestRequest;
import org.elasticsearch.client.advanced.delete.DeleteRestResponse;

import java.io.IOException;
import java.util.Map;

public class AdvancedRestClient {

    private RestClient lowLevelClient;

    // TODO We will need to know as soon as we start this client, what is the version of the cluster
    // Some APIS will need to have different parameters depending on 1.x, 2.x, 5.x, ...
    // It's not needed at the beginning of this project

    public AdvancedRestClient(RestClient lowLevelClient) {
        this.lowLevelClient = lowLevelClient;
    }

    /**
     * TODO remove
     * Delete a single document
     * @param request The document to be deleted
     * @return Elasticsearch response
     * @throws IOException In case something is wrong. Can be a ResponseException as well.
     */
    public String deleteAsString(DeleteRestRequest request) throws IOException {
        return new DeleteRestOperation().executeAsString(lowLevelClient, request);
    }

    /**
     * TODO replace with delete(DeleteRestRequest)
     * Delete a single document
     * @param request The document to be deleted
     * @return Elasticsearch response
     * @throws IOException In case something is wrong. Can be a ResponseException as well.
     */
    public Map<String, Object> deleteAsMap(DeleteRestRequest request) throws IOException {
        return new DeleteRestOperation().executeAsMap(lowLevelClient, request);
    }

    /**
     * Delete a single document
     * @param request The document to be deleted
     * @return Elasticsearch response
     * @throws IOException In case something is wrong. Can be a ResponseException as well.
     */
    public DeleteRestResponse delete(DeleteRestRequest request) throws IOException {
        return new DeleteRestOperation().executeAsObject(lowLevelClient, request);
    }

    /**
     * Delete a single document and call a listener when done
     * @param request The document to be deleted
     * @param listener Listener to call when operation is done or in case of failure.
     * @throws IOException In case something is wrong.
     */
    public void delete(DeleteRestRequest request, RestResponseListener<DeleteRestResponse> listener) throws IOException {
        new DeleteRestOperation().execute(lowLevelClient, request, listener);
    }
}
