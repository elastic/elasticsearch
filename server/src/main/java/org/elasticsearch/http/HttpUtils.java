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
package org.elasticsearch.http;

import java.util.List;

class HttpUtils {

    /**
     * Returns if the request connection should be closed on completion.
     */
    static boolean isCloseConnection(HttpRequest request) {
        final boolean http10 = isHttp10(request);
        String connectionHeader = getHeader(request, DefaultRestChannel.CONNECTION);
        return DefaultRestChannel.CLOSE.equalsIgnoreCase(connectionHeader) ||
            (http10 && !DefaultRestChannel.KEEP_ALIVE.equalsIgnoreCase(connectionHeader));
    }

    // Determine if the request protocol version is HTTP 1.0
    private static boolean isHttp10(HttpRequest request) {
        return request.protocolVersion() == HttpRequest.HttpVersion.HTTP_1_0;
    }

    private static String getHeader(HttpRequest request, String header) {
        List<String> values = request.getHeaders().get(header);
        if (values != null && values.isEmpty() == false) {
            return values.get(0);
        }
        return null;
    }
}
