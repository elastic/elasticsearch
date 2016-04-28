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

import org.apache.commons.logging.Log;
import org.apache.http.RequestLine;
import org.apache.http.StatusLine;

import java.io.IOException;

/**
 * Helper class that exposes static method to unify the way requests are logged
 */
public final class RequestLogger {

    private RequestLogger() {
    }

    /**
     * Logs a request that yielded a response
     */
    public static void log(Log logger, String message, RequestLine requestLine, Node node, StatusLine statusLine) {
        logger.debug(message + " [" + requestLine.getMethod() + " " + node.getHttpHost() +
                requestLine.getUri() + "] [" + statusLine + "]");
    }

    /**
     * Logs a request that failed
     */
    public static void log(Log logger, String message, RequestLine requestLine, Node node, IOException e) {
        logger.debug(message + " [" + requestLine.getMethod() + " " + node.getHttpHost() +
                requestLine.getUri() + "]", e);
    }
}
