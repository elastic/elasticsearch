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

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolVersion;
import org.apache.http.RequestLine;
import org.apache.http.StatusLine;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicRequestLine;
import org.apache.http.message.BasicStatusLine;

import java.nio.charset.StandardCharsets;

public class RestClientTestUtil {
    public static Response mockResponse(String responseBody) {
        ProtocolVersion protocolVersion = new ProtocolVersion("HTTP", 1, 1);
        RequestLine requestLine = new BasicRequestLine("GET", "/", protocolVersion);
        StatusLine statusLine = new BasicStatusLine(protocolVersion, 200, "OK");
        HttpResponse httpResponse = new BasicHttpResponse(statusLine);
        httpResponse.setEntity(new StringEntity(responseBody, StandardCharsets.UTF_8));
        httpResponse.addHeader("Content-Type", ContentType.APPLICATION_JSON.getMimeType());
        return new Response(requestLine, new HttpHost("localhost", 9200), httpResponse);
    }
}
