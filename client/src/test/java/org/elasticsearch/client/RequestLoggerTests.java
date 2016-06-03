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

import com.carrotsearch.randomizedtesting.generators.RandomInts;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpHost;
import org.apache.http.ProtocolVersion;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpTrace;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicStatusLine;
import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.CoreMatchers.equalTo;

public class RequestLoggerTests extends LuceneTestCase {

    public void testTraceRequest() throws IOException, URISyntaxException {
        HttpHost host = new HttpHost("localhost", 9200, random().nextBoolean() ? "http" : "https");
        URI uri = new URI("/index/type/_api");

        HttpRequestBase request;
        int requestType = RandomInts.randomIntBetween(random(), 0, 7);
        switch(requestType) {
            case 0:
                request = new HttpGetWithEntity(uri);
                break;
            case 1:
                request = new HttpPost(uri);
                break;
            case 2:
                request = new HttpPut(uri);
                break;
            case 3:
                request = new HttpDeleteWithEntity(uri);
                break;
            case 4:
                request = new HttpHead(uri);
                break;
            case 5:
                request = new HttpTrace(uri);
                break;
            case 6:
                request = new HttpOptions(uri);
                break;
            case 7:
                request = new HttpPatch(uri);
                break;
            default:
                throw new UnsupportedOperationException();
        }

        String expected = "curl -iX " + request.getMethod() + " '" + host + uri + "'";

        if (request instanceof HttpEntityEnclosingRequest && random().nextBoolean()) {
            HttpEntityEnclosingRequest enclosingRequest = (HttpEntityEnclosingRequest) request;
            String requestBody = "{ \"field\": \"value\" }";
            enclosingRequest.setEntity(new StringEntity(requestBody, StandardCharsets.UTF_8));
            expected += " -d '" + requestBody + "'";
        }

        String traceRequest = RequestLogger.buildTraceRequest(request, host);
        assertThat(traceRequest, equalTo(expected));
    }

    public void testTraceResponse() throws IOException {
        ProtocolVersion protocolVersion = new ProtocolVersion("HTTP", 1, 1);
        int statusCode = RandomInts.randomIntBetween(random(), 200, 599);
        String reasonPhrase = "REASON";
        BasicStatusLine statusLine = new BasicStatusLine(protocolVersion, statusCode, reasonPhrase);
        String expected = "# " + statusLine.toString();
        BasicHttpResponse httpResponse = new BasicHttpResponse(statusLine);
        int numHeaders = RandomInts.randomIntBetween(random(), 0, 3);
        for (int i = 0; i < numHeaders; i++) {
            httpResponse.setHeader("header" + i, "value");
            expected += "\n# header" + i + ": value";
        }
        expected += "\n#";
        if (random().nextBoolean()) {
            String responseBody = "{\n  \"field\": \"value\"\n}";
            httpResponse.setEntity(new StringEntity(responseBody, StandardCharsets.UTF_8));
            expected += "\n# {";
            expected += "\n#   \"field\": \"value\"";
            expected += "\n# }";
        }

        String traceResponse = RequestLogger.buildTraceResponse(httpResponse);
        assertThat(traceResponse, equalTo(expected));
    }
}
