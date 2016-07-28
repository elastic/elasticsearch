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
import org.apache.http.HttpEntity;
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
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.nio.entity.NByteArrayEntity;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class RequestLoggerTests extends RestClientTestCase {

    public void testTraceRequest() throws IOException, URISyntaxException {
        HttpHost host = new HttpHost("localhost", 9200, getRandom().nextBoolean() ? "http" : "https");

        String expectedEndpoint = "/index/type/_api";
        URI uri;
        if (randomBoolean()) {
            uri = new URI(expectedEndpoint);
        } else {
            uri = new URI("index/type/_api");
        }

        HttpRequestBase request;
        int requestType = RandomInts.randomIntBetween(getRandom(), 0, 7);
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

        String expected = "curl -iX " + request.getMethod() + " '" + host + expectedEndpoint + "'";
        boolean hasBody = request instanceof HttpEntityEnclosingRequest && getRandom().nextBoolean();
        String requestBody = "{ \"field\": \"value\" }";
        if (hasBody) {
            expected += " -d '" + requestBody + "'";
            HttpEntityEnclosingRequest enclosingRequest = (HttpEntityEnclosingRequest) request;
            HttpEntity entity;
            switch(RandomInts.randomIntBetween(getRandom(), 0, 3)) {
                case 0:
                    entity = new StringEntity(requestBody, StandardCharsets.UTF_8);
                    break;
                case 1:
                    entity = new InputStreamEntity(new ByteArrayInputStream(requestBody.getBytes(StandardCharsets.UTF_8)));
                    break;
                case 2:
                    entity = new NStringEntity(requestBody, StandardCharsets.UTF_8);
                    break;
                case 3:
                    entity = new NByteArrayEntity(requestBody.getBytes(StandardCharsets.UTF_8));
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
            enclosingRequest.setEntity(entity);
        }
        String traceRequest = RequestLogger.buildTraceRequest(request, host);
        assertThat(traceRequest, equalTo(expected));
        if (hasBody) {
            //check that the body is still readable as most entities are not repeatable
            String body = EntityUtils.toString(((HttpEntityEnclosingRequest) request).getEntity(), StandardCharsets.UTF_8);
            assertThat(body, equalTo(requestBody));
        }
    }

    public void testTraceResponse() throws IOException {
        ProtocolVersion protocolVersion = new ProtocolVersion("HTTP", 1, 1);
        int statusCode = RandomInts.randomIntBetween(getRandom(), 200, 599);
        String reasonPhrase = "REASON";
        BasicStatusLine statusLine = new BasicStatusLine(protocolVersion, statusCode, reasonPhrase);
        String expected = "# " + statusLine.toString();
        BasicHttpResponse httpResponse = new BasicHttpResponse(statusLine);
        int numHeaders = RandomInts.randomIntBetween(getRandom(), 0, 3);
        for (int i = 0; i < numHeaders; i++) {
            httpResponse.setHeader("header" + i, "value");
            expected += "\n# header" + i + ": value";
        }
        expected += "\n#";
        boolean hasBody = getRandom().nextBoolean();
        String responseBody = "{\n  \"field\": \"value\"\n}";
        if (hasBody) {
            expected += "\n# {";
            expected += "\n#   \"field\": \"value\"";
            expected += "\n# }";
            HttpEntity entity;
            if (getRandom().nextBoolean()) {
                entity = new StringEntity(responseBody, StandardCharsets.UTF_8);
            } else {
                //test a non repeatable entity
                entity = new InputStreamEntity(new ByteArrayInputStream(responseBody.getBytes(StandardCharsets.UTF_8)));
            }
            httpResponse.setEntity(entity);
        }
        String traceResponse = RequestLogger.buildTraceResponse(httpResponse);
        assertThat(traceResponse, equalTo(expected));
        if (hasBody) {
            //check that the body is still readable as most entities are not repeatable
            String body = EntityUtils.toString(httpResponse.getEntity(), StandardCharsets.UTF_8);
            assertThat(body, equalTo(responseBody));
        }
    }
}
