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

package org.elasticsearch.test.rest.yaml;

import org.elasticsearch.client.ResponseException;

import java.io.IOException;

/**
 * Exception obtained from a REST call in case the response code indicated an error. Eagerly reads the response body into a string
 * for later optional parsing. Supports parsing the response body when needed and returning specific values extracted from it.
 */
public class ClientYamlTestResponseException extends IOException {

    private final ClientYamlTestResponse restTestResponse;
    private final ResponseException responseException;

    public ClientYamlTestResponseException(ResponseException responseException) throws IOException {
        super(responseException);
        this.responseException = responseException;
        this.restTestResponse = new ClientYamlTestResponse(responseException.getResponse());
    }

    /**
     * Exposes the obtained response body
     */
    public ClientYamlTestResponse getRestTestResponse() {
        return restTestResponse;
    }

    /**
     * Exposes the origina {@link ResponseException}. Note that the entity will always be null as it
     * gets eagerly consumed and exposed through {@link #getRestTestResponse()}.
     */
    public ResponseException getResponseException() {
        return responseException;
    }
}
