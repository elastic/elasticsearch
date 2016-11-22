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

import com.fasterxml.jackson.jr.ob.JSON;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Delete a document
 */
public abstract class RestOperation<Req extends RestRequest, Resp extends RestResponse> {

    protected abstract Response doExecute(RestClient client, Req request) throws IOException;
    protected abstract void doExecute(RestClient client, Req request, ResponseListener listener) throws IOException;
    protected abstract Resp toRestResponse(Map<String, Object> response) throws IOException;

    public static Map<String, Object> toMap(Response response) throws IOException {
        return JSON.std.mapFrom(response.getEntity().getContent());
    }

    public Resp execute(RestClient client, Req request) throws IOException {
        validate(request);
        return toRestResponse(toMap(doExecute(client, request)));
    }

    public void execute(RestClient client, Req request, Consumer<Resp> responseConsumer, Consumer<Exception> failureConsumer)
        throws IOException {
        validate(request);
        doExecute(client, request, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                try {
                    responseConsumer.accept(toRestResponse(toMap(response)));
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

    private void validate(Req request) {
        if (request == null) {
            throw new IllegalArgumentException("Request can not be null");
        }
        request.validate();
    }
}
