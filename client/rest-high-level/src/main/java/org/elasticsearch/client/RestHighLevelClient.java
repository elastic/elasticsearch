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

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.main.MainRequest;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;

/**
 * High level REST client that wraps an instance of the low level {@link RestClient} and allows to build requests and read responses.
 * The provided {@link RestClient} is externally built and closed.
 */
public class RestHighLevelClient {

    private final RestClient client;

    public RestHighLevelClient(RestClient client) {
        this.client = Objects.requireNonNull(client);
    }

    /**
     * Pings the remote Elasticsearch cluster and returns true if the ping succeeded, false otherwise
     */
    public boolean ping(Header... headers) throws IOException {
        return performRequest(new MainRequest(), (request) -> Request.ping(), RestHighLevelClient::convertExistsResponse, headers);
    }

    /**
     * Retrieves a document by id using the get api
     */
    public GetResponse get(GetRequest getRequest, Header... headers) throws IOException {
        return performRequestAndParseEntity(getRequest, Request::get, GetResponse::fromXContent, headers);
    }

    /**
     * Asynchronously retrieves a document by id using the get api
     */
    public void getAsync(GetRequest getRequest, ActionListener<GetResponse> listener, Header... headers) {
        performRequestAsyncAndParseEntity(getRequest, Request::get, GetResponse::fromXContent, listener, headers);
    }

    /**
     * Checks for the existence of a document. Returns true if it exists, false otherwise
     */
    public boolean exists(GetRequest getRequest, Header... headers) throws IOException {
        return performRequest(getRequest, Request::get, RestHighLevelClient::convertExistsResponse, headers);
    }

    /**
     * Asynchronously checks for the existence of a document. Returns true if it exists, false otherwise
     */
    public void existsAsync(GetRequest getRequest, ActionListener<Boolean> listener, Header... headers) {
        performRequestAsync(getRequest, Request::get, RestHighLevelClient::convertExistsResponse, listener, headers);
    }

    private <Req extends ActionRequest, Resp> Resp performRequestAndParseEntity(Req request, Function<Req, Request>  requestConverter,
            CheckedFunction<XContentParser, Resp, IOException> entityParser, Header... headers) throws IOException {
        return performRequest(request, requestConverter, (response) -> parseEntity(response.getEntity(), entityParser), headers);
    }

    <Req extends ActionRequest, Resp> Resp performRequest(Req request, Function<Req, Request> requestConverter,
            CheckedFunction<Response, Resp, IOException> responseConverter, Header... headers) throws IOException {

        ActionRequestValidationException validationException = request.validate();
        if (validationException != null) {
            throw validationException;
        }
        Request req = requestConverter.apply(request);
        Response response;
        try {
            response = client.performRequest(req.method, req.endpoint, req.params, req.entity, headers);
        } catch (ResponseException e) {
            throw parseResponseException(e);
        }
        return responseConverter.apply(response);
    }

    private <Req extends ActionRequest, Resp> void performRequestAsyncAndParseEntity(Req request, Function<Req, Request> requestConverter,
            CheckedFunction<XContentParser, Resp, IOException> entityParser, ActionListener<Resp> listener, Header... headers) {
        performRequestAsync(request, requestConverter, (response) -> parseEntity(response.getEntity(), entityParser), listener, headers);
    }

    <Req extends ActionRequest, Resp> void performRequestAsync(Req request, Function<Req, Request> requestConverter,
            CheckedFunction<Response, Resp, IOException> responseConverter, ActionListener<Resp> listener, Header... headers) {
        ActionRequestValidationException validationException = request.validate();
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }
        Request req = requestConverter.apply(request);
        ResponseListener responseListener = wrapResponseListener(responseConverter, listener);
        client.performRequestAsync(req.method, req.endpoint, req.params, req.entity, responseListener, headers);
    }

    static <Resp> ResponseListener wrapResponseListener(CheckedFunction<Response, Resp, IOException> responseConverter,
                                                        ActionListener<Resp> actionListener) {
        return new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                try {
                    actionListener.onResponse(responseConverter.apply(response));
                } catch(Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception exception) {
                if (exception instanceof ResponseException) {
                    ResponseException responseException = (ResponseException) exception;
                    actionListener.onFailure(parseResponseException(responseException));
                } else {
                    actionListener.onFailure(exception);
                }
            }
        };
    }

    static ElasticsearchException parseResponseException(ResponseException responseException) {
        Response response = responseException.getResponse();
        HttpEntity entity = response.getEntity();
        ElasticsearchException elasticsearchException;
        if (entity == null) {
            elasticsearchException = new ElasticsearchStatusException(
                    responseException.getMessage(), RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        } else {
            try {
                elasticsearchException = parseEntity(entity, BytesRestResponse::errorFromXContent);
            } catch (IOException e) {
                RestStatus restStatus = RestStatus.fromCode(response.getStatusLine().getStatusCode());
                elasticsearchException = new ElasticsearchStatusException("unable to parse response body", restStatus, e);
            }
        }
        elasticsearchException.addSuppressed(responseException);
        return elasticsearchException;
    }

    static <Resp> Resp parseEntity(
            HttpEntity entity, CheckedFunction<XContentParser, Resp, IOException> entityParser) throws IOException {
        if (entity == null) {
            throw new IllegalStateException("Response body expected but not returned");
        }
        if (entity.getContentType() == null) {
            throw new IllegalStateException("Elasticsearch didn't return the [Content-Type] header, unable to parse response body");
        }
        XContentType xContentType = XContentType.fromMediaTypeOrFormat(entity.getContentType().getValue());
        if (xContentType == null) {
            throw new IllegalStateException("Unsupported Content-Type: " + entity.getContentType().getValue());
        }
        try (XContentParser parser = xContentType.xContent().createParser(NamedXContentRegistry.EMPTY, entity.getContent())) {
            return entityParser.apply(parser);
        }
    }

    static boolean convertExistsResponse(Response response) {
        return response.getStatusLine().getStatusCode() == 200;
    }
}
