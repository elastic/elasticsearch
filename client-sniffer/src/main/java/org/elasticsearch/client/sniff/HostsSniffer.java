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

package org.elasticsearch.client.sniff;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.elasticsearch.client.ElasticsearchResponse;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Class responsible for sniffing the http hosts from elasticsearch through the nodes info api and returning them back.
 * Compatible with elasticsearch 5.x and 2.x.
 */
public class HostsSniffer {

    private static final Log logger = LogFactory.getLog(HostsSniffer.class);

    private final RestClient restClient;
    private final Map<String, String> sniffRequestParams;
    private final Scheme scheme;
    private final JsonFactory jsonFactory = new JsonFactory();

    protected HostsSniffer(RestClient restClient, long sniffRequestTimeout, Scheme scheme) {
        this.restClient = restClient;
        this.sniffRequestParams = Collections.<String, String>singletonMap("timeout", sniffRequestTimeout + "ms");
        this.scheme = scheme;
    }

    /**
     * Calls the elasticsearch nodes info api, parses the response and returns all the found http hosts
     */
    public List<HttpHost> sniffHosts() throws IOException {
        try (ElasticsearchResponse response = restClient.performRequest("get", "/_nodes/http", sniffRequestParams, null)) {
            return readHosts(response.getEntity());
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
                            HttpHost sniffedHost = readHost(nodeId, parser, this.scheme);
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

    private static HttpHost readHost(String nodeId, JsonParser parser, Scheme scheme) throws IOException {
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

    /**
     * Returns a new {@link Builder} to help with {@link HostsSniffer} creation.
     */
    public static Builder builder(RestClient restClient) {
        return new Builder(restClient);
    }

    public enum Scheme {
        HTTP("http"), HTTPS("https");

        private final String name;

        Scheme(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /**
     * HostsSniffer builder. Helps creating a new {@link HostsSniffer}.
     */
    public static class Builder {
        public static final long DEFAULT_SNIFF_REQUEST_TIMEOUT = TimeUnit.SECONDS.toMillis(1);

        private final RestClient restClient;
        private long sniffRequestTimeout = DEFAULT_SNIFF_REQUEST_TIMEOUT;
        private Scheme scheme;

        private Builder(RestClient restClient) {
            Objects.requireNonNull(restClient, "restClient cannot be null");
            this.restClient = restClient;
        }

        /**
         * Sets the sniff request timeout to be passed in as a query string parameter to elasticsearch.
         * Allows to halt the request without any failure, as only the nodes that have responded
         * within this timeout will be returned.
         */
        public Builder setSniffRequestTimeout(int sniffRequestTimeout) {
            if (sniffRequestTimeout <= 0) {
                throw new IllegalArgumentException("sniffRequestTimeout must be greater than 0");
            }
            this.sniffRequestTimeout = sniffRequestTimeout;
            return this;
        }

        /**
         * Sets the scheme to associate sniffed nodes with (as it is not returned by elasticsearch)
         */
        public Builder setScheme(Scheme scheme) {
            Objects.requireNonNull(scheme, "scheme cannot be null");
            this.scheme = scheme;
            return this;
        }

        /**
         * Creates a new {@link HostsSniffer} instance given the provided configuration
         */
        public HostsSniffer build() {
            return new HostsSniffer(restClient, sniffRequestTimeout, scheme);
        }
    }
}
