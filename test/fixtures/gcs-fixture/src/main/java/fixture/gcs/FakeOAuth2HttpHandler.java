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
package fixture.gcs;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

import static java.nio.charset.StandardCharsets.UTF_8;

@SuppressForbidden(reason = "Uses a HttpServer to emulate a fake OAuth2 authentication service")
public class FakeOAuth2HttpHandler implements HttpHandler {

    private static final byte[] BUFFER = new byte[1024];

    @Override
    public void handle(final HttpExchange exchange) throws IOException {
        try {
            while (exchange.getRequestBody().read(BUFFER) >= 0) ;
            byte[] response = ("{\"access_token\":\"foo\",\"token_type\":\"Bearer\",\"expires_in\":3600}").getBytes(UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
            exchange.getResponseBody().write(response);
        } finally {
            int read = exchange.getRequestBody().read();
            assert read == -1 : "Request body should have been fully read here but saw [" + read + "]";
            exchange.close();
        }
    }
}
