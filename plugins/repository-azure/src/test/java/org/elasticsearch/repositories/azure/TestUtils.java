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
package org.elasticsearch.repositories.azure;

import com.microsoft.azure.storage.Constants;
import com.microsoft.azure.storage.StorageErrorCodeStrings;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

final class TestUtils {

    private TestUtils() {}

    @SuppressForbidden(reason = "use HttpExchange and Headers")
    static void sendError(final HttpExchange exchange, final RestStatus status) throws IOException {
        final Headers headers = exchange.getResponseHeaders();
        headers.add("Content-Type", "application/xml");

        final String requestId = exchange.getRequestHeaders().getFirst(Constants.HeaderConstants.CLIENT_REQUEST_ID_HEADER);
        if (requestId != null) {
            headers.add(Constants.HeaderConstants.REQUEST_ID_HEADER, requestId);
        }

        final String errorCode = toAzureErrorCode(status);
        if (errorCode != null) {
            headers.add(Constants.HeaderConstants.ERROR_CODE, errorCode);
        }

        if (errorCode == null || "HEAD".equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(status.getStatus(), -1L);
        } else {
            final byte[] response = ("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>" + errorCode + "</Code><Message>"
                + status + "</Message></Error>").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status.getStatus(), response.length);
            exchange.getResponseBody().write(response);
        }
    }

    // See https://docs.microsoft.com/en-us/rest/api/storageservices/common-rest-api-error-codes
    private static String toAzureErrorCode(final RestStatus status) {
        assert status.getStatus() >= 400;
        switch (status) {
            case BAD_REQUEST:
                return StorageErrorCodeStrings.INVALID_METADATA;
            case NOT_FOUND:
                return StorageErrorCodeStrings.BLOB_NOT_FOUND;
            case INTERNAL_SERVER_ERROR:
                return StorageErrorCodeStrings.INTERNAL_ERROR;
            default:
                return null;
        }
    }
}
