/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.http;

public class HttpUtils {

    static final String CLOSE = "close";
    static final String CONNECTION = "connection";
    static final String KEEP_ALIVE = "keep-alive";

    // Determine if the request connection should be closed on completion.
    public static boolean shouldCloseConnection(HttpRequest httpRequest) {
        try {
            final boolean http10 = httpRequest.protocolVersion() == HttpRequest.HttpVersion.HTTP_1_0;
            return CLOSE.equalsIgnoreCase(httpRequest.header(CONNECTION))
                || (http10 && !KEEP_ALIVE.equalsIgnoreCase(httpRequest.header(CONNECTION)));
        } catch (Exception e) {
            // In case we fail to parse the http protocol version out of the request we always close the connection
            return true;
        }
    }
}
