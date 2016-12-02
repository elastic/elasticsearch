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

import com.fasterxml.jackson.jr.ob.JSON;
import org.apache.http.HttpHost;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.highlevel.delete.DeleteRestRequest;
import org.elasticsearch.client.highlevel.delete.DeleteRestResponse;
import org.elasticsearch.client.highlevel.get.GetRestRequest;
import org.elasticsearch.client.highlevel.get.GetRestResponse;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

public class HighlevelClient implements Closeable {

    private RestClient restClient;

    // TODO We will need to know as soon as we start this client, what is the version of the cluster
    // Some APIS will need to have different parameters depending on 1.x, 2.x, 5.x, ...
    // It's not needed at the beginning of this project

    public HighlevelClient(String host, int port) {
        this.restClient = RestClient.builder(new HttpHost(host, port)).build();
    }

    public HighlevelClient(RestClient restClient) {
        this.restClient = Objects.requireNonNull(restClient);
    }

    @Override
    public void close() throws IOException {
        this.restClient.close();
        this.restClient = null;
    }

    public SearchResponse performSearchRequest(SearchRequest request) throws IOException {
        StringEntity entity = new StringEntity(request.searchSource().toString());
        return new SearchResponse(this.restClient.performRequest("GET", buildSearchEndpoint(request), request.urlParams, entity));
    }

    public void performSearchRequestAsync(SearchRequest request, ResponseListener responseListener) throws IOException {
        StringEntity entity = new StringEntity(request.searchSource().toString());
        this.restClient.performRequestAsync("GET", buildSearchEndpoint(request), request.urlParams, entity, responseListener);
    }

    /**
     * Delete a single document
     * @param request The document to be deleted
     * @return Elasticsearch response
     * @throws IOException In case something is wrong. Can be a ResponseException as well.
     */
    public DeleteRestResponse delete(DeleteRestRequest request) throws IOException {
        if (request == null) {
            throw new IllegalArgumentException("Request can not be null");
        }
        request.validate();
        return toDeleteRestResponse(
            restClient.performRequest("DELETE", "/" + request.getIndex() + "/" + request.getType() + "/" + request.getId())
        );
    }

    /**
     * Delete a single document and call a listener when done
     * @param request The document to be deleted
     * @param responseConsumer Listener to call when operation is done or in case of failure.
     * @param failureConsumer Listener to call in case of failure.
     * @throws IOException In case something is wrong.
     */
    public void delete(DeleteRestRequest request,
                       Consumer<DeleteRestResponse> responseConsumer,
                       Consumer<Exception> failureConsumer) throws IOException {
        if (request == null) {
            throw new IllegalArgumentException("Request can not be null");
        }
        request.validate();
        restClient.performRequestAsync("DELETE",
            "/" + request.getIndex() + "/" + request.getType() + "/" + request.getId(), new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    try {
                        responseConsumer.accept(toDeleteRestResponse(response));
                    } catch (IOException e) {
                        failureConsumer.accept(e);
                    }
                }

                @Override
                public void onFailure(Exception exception) {
                    failureConsumer.accept(exception);
                }
            });
    }

    public static DeleteRestResponse toDeleteRestResponse(Response response) throws IOException {
        // Read from the map as we don't want to use reflection
        Map<String, Object> map = toMap(response);
        DeleteRestResponse restResponse = new DeleteRestResponse();
        boolean found = (boolean) map.get("found");
        restResponse.setFound(found);
        return restResponse;
    }

    /**
     * Get a single document
     * @param request The document to be deleted
     * @return Elasticsearch response
     * @throws IOException In case something is wrong. Can be a ResponseException as well.
     */
    public GetRestResponse get(GetRestRequest request) throws IOException {
        if (request == null) {
            throw new IllegalArgumentException("Request can not be null");
        }
        request.validate();
        return toGetRestResponse(restClient.performRequest("GET",
            "/" + request.getIndex() + "/" + request.getType() + "/" + request.getId()));
    }

    /**
     * Delete a single document and call a listener when done
     * @param request The document to be deleted
     * @param responseConsumer Listener to call when operation is done or in case of failure.
     * @param failureConsumer Listener to call in case of failure.
     * @throws IOException In case something is wrong.
     */
    public void get(GetRestRequest request,
                    Consumer<GetRestResponse> responseConsumer,
                    Consumer<Exception> failureConsumer) throws IOException {
        if (request == null) {
            throw new IllegalArgumentException("Request can not be null");
        }
        request.validate();
        restClient.performRequestAsync("GET",
            "/" + request.getIndex() + "/" + request.getType() + "/" + request.getId(), new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    try {
                        responseConsumer.accept(toGetRestResponse(response));
                    } catch (IOException e) {
                        failureConsumer.accept(e);
                    }
                }

                @Override
                public void onFailure(Exception exception) {
                    failureConsumer.accept(exception);
                }
            });
    }

    @SuppressWarnings("unchecked")
    public static GetRestResponse toGetRestResponse(Response response) throws IOException {
        // Read from the map as we don't want to use reflection
        Map<String, Object> map = toMap(response);
        GetRestResponse restResponse = new GetRestResponse();
        restResponse.setFound((boolean) map.get("found"));
        Object objSource = map.get("_source");
        restResponse.setSource((Map<String, Object>) objSource);
        restResponse.setIndex((String) map.get("_index"));
        restResponse.setType((String) map.get("_type"));
        restResponse.setId((String) map.get("_id"));
        restResponse.setVersion((Integer) map.get("_version"));
        return restResponse;
    }

    private static Map<String, Object> toMap(Response response) throws IOException {
        return JSON.std.mapFrom(response.getEntity().getContent());
    }

    private static String buildSearchEndpoint(SearchRequest request) {
        String indices = String.join(",", request.indices());
        if (indices.length() > 0) {
            indices = indices + "/";
        }
        String types = String.join(",", request.types());
        if (types.length() > 0) {
            indices = indices + "/";
        }
        return "/" + indices + types + "_search";
    }
}
