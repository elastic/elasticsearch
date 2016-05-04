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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Calls nodes info api and returns a list of http hosts extracted from it
 */
//TODO this could potentially a call to _cat/nodes (although it doesn't support timeout param), but how would we handle bw comp with 2.x?
final class Sniffer {

    private static final Log logger = LogFactory.getLog(Sniffer.class);

    private final CloseableHttpClient client;
    private final RequestConfig sniffRequestConfig;
    private final int sniffRequestTimeout;
    private final String scheme;
    private final JsonFactory jsonFactory;

    Sniffer(CloseableHttpClient client, RequestConfig sniffRequestConfig, int sniffRequestTimeout, String scheme) {
        Objects.requireNonNull(client, "client cannot be null");
        Objects.requireNonNull(sniffRequestConfig, "sniffRequestConfig cannot be null");
        if (sniffRequestTimeout <=0) {
            throw new IllegalArgumentException("sniffRequestTimeout must be greater than 0");
        }
        Objects.requireNonNull(scheme, "scheme cannot be null");
        this.client = client;
        this.sniffRequestConfig = sniffRequestConfig;
        this.sniffRequestTimeout = sniffRequestTimeout;
        this.scheme = scheme;
        this.jsonFactory = new JsonFactory();
    }

    List<HttpHost> sniffNodes(HttpHost host) throws IOException {
        HttpGet httpGet = new HttpGet("/_nodes/http?timeout=" + sniffRequestTimeout + "ms");
        httpGet.setConfig(sniffRequestConfig);

        try (CloseableHttpResponse response = client.execute(host, httpGet)) {
            StatusLine statusLine = response.getStatusLine();
            if (statusLine.getStatusCode() >= 300) {
                RequestLogger.log(logger, "sniff failed", httpGet.getRequestLine(), host, statusLine);
                EntityUtils.consume(response.getEntity());
                throw new ElasticsearchResponseException(httpGet.getRequestLine(), host, statusLine);
            } else {
                List<HttpHost> nodes = readHosts(response.getEntity());
                RequestLogger.log(logger, "sniff succeeded", httpGet.getRequestLine(), host, statusLine);
                return nodes;
            }
        } catch(IOException e) {
            RequestLogger.log(logger, "sniff failed", httpGet.getRequestLine(), host, e);
            throw e;
        }
    }

    private List<HttpHost> readHosts(HttpEntity entity) throws IOException {
        try (InputStream inputStream = entity.getContent()) {
            JsonParser parser = jsonFactory.createParser(inputStream);
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                throw new IOException("expected data to start with an object");
            }
            List<HttpHost> hosts = new ArrayList<>();
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                if (parser.getCurrentToken() == JsonToken.START_OBJECT) {
                    if ("nodes".equals(parser.getCurrentName())) {
                        while (parser.nextToken() != JsonToken.END_OBJECT) {
                            JsonToken token = parser.nextToken();
                            assert token == JsonToken.START_OBJECT;
                            String nodeId = parser.getCurrentName();
                            HttpHost sniffedHost = readNode(nodeId, parser, this.scheme);
                            if (sniffedHost != null) {
                                logger.trace("adding node [" + nodeId + "]");
                                hosts.add(sniffedHost);
                            }
                        }
                    } else {
                        parser.skipChildren();
                    }
                }
            }
            return hosts;
        }
    }

    private static HttpHost readNode(String nodeId, JsonParser parser, String scheme) throws IOException {
        HttpHost httpHost = null;
        String fieldName = null;
        while (parser.nextToken() != JsonToken.END_OBJECT) {
            if (parser.getCurrentToken() == JsonToken.FIELD_NAME) {
                fieldName = parser.getCurrentName();
            } else if (parser.getCurrentToken() == JsonToken.START_OBJECT) {
                if ("http".equals(fieldName)) {
                    while (parser.nextToken() != JsonToken.END_OBJECT) {
                        if (parser.getCurrentToken() == JsonToken.VALUE_STRING && "publish_address".equals(parser.getCurrentName())) {
                            URI boundAddressAsURI = URI.create(scheme + "://" + parser.getValueAsString());
                            httpHost = new HttpHost(boundAddressAsURI.getHost(), boundAddressAsURI.getPort(),
                                    boundAddressAsURI.getScheme());
                        } else if (parser.getCurrentToken() == JsonToken.START_OBJECT) {
                            parser.skipChildren();
                        }
                    }
                } else {
                    parser.skipChildren();
                }
            }
        }
        //http section is not present if http is not enabled on the node, ignore such nodes
        if (httpHost == null) {
            logger.debug("skipping node [" + nodeId + "] with http disabled");
            return null;
        }
        return httpHost;
    }
}
