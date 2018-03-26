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
import org.elasticsearch.client.Node;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.Node.Roles;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Class responsible for sniffing the http hosts from elasticsearch through the nodes info api and returning them back.
 * Compatible with elasticsearch 2.x+.
 */
public final class ElasticsearchHostsSniffer implements HostsSniffer {

    private static final Log logger = LogFactory.getLog(ElasticsearchHostsSniffer.class);

    public static final long DEFAULT_SNIFF_REQUEST_TIMEOUT = TimeUnit.SECONDS.toMillis(1);

    private final RestClient restClient;
    private final Map<String, String> sniffRequestParams;
    private final Scheme scheme;
    private final JsonFactory jsonFactory = new JsonFactory();

    /**
     * Creates a new instance of the Elasticsearch sniffer. It will use the provided {@link RestClient} to fetch the hosts,
     * through the nodes info api, the default sniff request timeout value {@link #DEFAULT_SNIFF_REQUEST_TIMEOUT} and http
     * as the scheme for all the hosts.
     * @param restClient client used to fetch the hosts from elasticsearch through nodes info api. Usually the same instance
     *                   that is also provided to {@link Sniffer#builder(RestClient)}, so that the hosts are set to the same
     *                   client that was used to fetch them.
     */
    public ElasticsearchHostsSniffer(RestClient restClient) {
        this(restClient, DEFAULT_SNIFF_REQUEST_TIMEOUT, ElasticsearchHostsSniffer.Scheme.HTTP);
    }

    /**
     * Creates a new instance of the Elasticsearch sniffer. It will use the provided {@link RestClient} to fetch the hosts
     * through the nodes info api, the provided sniff request timeout value and scheme.
     * @param restClient client used to fetch the hosts from elasticsearch through nodes info api. Usually the same instance
     *                   that is also provided to {@link Sniffer#builder(RestClient)}, so that the hosts are set to the same
     *                   client that was used to sniff them.
     * @param sniffRequestTimeoutMillis the sniff request timeout (in milliseconds) to be passed in as a query string parameter
     *                                  to elasticsearch. Allows to halt the request without any failure, as only the nodes
     *                                  that have responded within this timeout will be returned.
     * @param scheme the scheme to associate sniffed nodes with (as it is not returned by elasticsearch)
     */
    public ElasticsearchHostsSniffer(RestClient restClient, long sniffRequestTimeoutMillis, Scheme scheme) {
        this.restClient = Objects.requireNonNull(restClient, "restClient cannot be null");
        if (sniffRequestTimeoutMillis < 0) {
            throw new IllegalArgumentException("sniffRequestTimeoutMillis must be greater than 0");
        }
        this.sniffRequestParams = Collections.<String, String>singletonMap("timeout", sniffRequestTimeoutMillis + "ms");
        this.scheme = Objects.requireNonNull(scheme, "scheme cannot be null");
    }

    /**
     * Calls the elasticsearch nodes info api, parses the response and returns all the found http hosts
     */
    @Override
    public List<Node> sniffHosts() throws IOException {
        Response response = restClient.performRequest("get", "/_nodes/http", sniffRequestParams);
        return readHosts(response.getEntity());
    }

    private List<Node> readHosts(HttpEntity entity) throws IOException {
        try (InputStream inputStream = entity.getContent()) {
            JsonParser parser = jsonFactory.createParser(inputStream);
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                throw new IOException("expected data to start with an object");
            }
            List<Node> nodes = new ArrayList<>();
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                if (parser.getCurrentToken() == JsonToken.START_OBJECT) {
                    if ("nodes".equals(parser.getCurrentName())) {
                        while (parser.nextToken() != JsonToken.END_OBJECT) {
                            JsonToken token = parser.nextToken();
                            assert token == JsonToken.START_OBJECT;
                            String nodeId = parser.getCurrentName();
                            readHost(nodeId, parser, scheme, nodes);
                        }
                    } else {
                        parser.skipChildren();
                    }
                }
            }
            return nodes;
        }
    }

    private static void readHost(String nodeId, JsonParser parser, Scheme scheme, List<Node> nodes) throws IOException {
        HttpHost publishedHost = null;
        List<HttpHost> boundHosts = new ArrayList<>();
        String fieldName = null;
        String version = null;
        boolean sawRoles = false;
        boolean master = false;
        boolean data = false;
        boolean ingest = false;
        while (parser.nextToken() != JsonToken.END_OBJECT) {
            if (parser.getCurrentToken() == JsonToken.FIELD_NAME) {
                fieldName = parser.getCurrentName();
            } else if (parser.getCurrentToken() == JsonToken.START_OBJECT) {
                if ("http".equals(fieldName)) {
                    while (parser.nextToken() != JsonToken.END_OBJECT) {
                        if (parser.getCurrentToken() == JsonToken.VALUE_STRING && "publish_address".equals(parser.getCurrentName())) {
                            URI publishAddressAsURI = URI.create(scheme + "://" + parser.getValueAsString());
                            publishedHost = new HttpHost(publishAddressAsURI.getHost(), publishAddressAsURI.getPort(),
                                    publishAddressAsURI.getScheme());
                        } else if (parser.currentToken() == JsonToken.START_ARRAY && "bound_address".equals(parser.getCurrentName())) {
                            while (parser.nextToken() != JsonToken.END_ARRAY) {
                                URI boundAddressAsURI = URI.create(scheme + "://" + parser.getValueAsString());
                                boundHosts.add(new HttpHost(boundAddressAsURI.getHost(), boundAddressAsURI.getPort(),
                                        boundAddressAsURI.getScheme()));
                            }
                        } else if (parser.getCurrentToken() == JsonToken.START_OBJECT) {
                            parser.skipChildren();
                        }
                    }
                } else {
                    parser.skipChildren();
                }
            } else if (parser.currentToken() == JsonToken.START_ARRAY) {
                if ("roles".equals(fieldName)) {
                    sawRoles = true;
                    while (parser.nextToken() != JsonToken.END_ARRAY) {
                        switch (parser.getText()) {
                        case "master":
                            master = true;
                            break;
                        case "data":
                            data = true;
                            break;
                        case "ingest":
                            ingest = true;
                            break;
                        default:
                            logger.warn("unknown role [" + parser.getText() + "] on node [" + nodeId + "]");
                        }
                    }
                } else {
                    parser.skipChildren();
                }
            } else if (parser.currentToken().isScalarValue()) {
                if ("version".equals(fieldName)) {
                    version = parser.getText();
                }
            }
        }
        //http section is not present if http is not enabled on the node, ignore such nodes
        if (publishedHost == null) {
            logger.debug("skipping node [" + nodeId + "] with http disabled");
        } else {
            logger.trace("adding node [" + nodeId + "]");
            assert sawRoles : "didn't see roles for [" + nodeId + "]";
            assert boundHosts.contains(publishedHost) :
                    "[" + nodeId + "] doesn't make sense! publishedHost should be in boundHosts";
            nodes.add(new Node(publishedHost, boundHosts, version, new Roles(master, data, ingest)));
        }
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
}
