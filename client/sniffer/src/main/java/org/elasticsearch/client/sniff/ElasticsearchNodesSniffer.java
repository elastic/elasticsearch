/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
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

package org.elasticsearch.client.sniff;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.Node.Roles;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;

/**
 * Class responsible for sniffing the http hosts from elasticsearch through the nodes info api and returning them back.
 * Compatible with elasticsearch 2.x+.
 */
public final class ElasticsearchNodesSniffer implements NodesSniffer {

    private static final Log logger = LogFactory.getLog(ElasticsearchNodesSniffer.class);

    public static final long DEFAULT_SNIFF_REQUEST_TIMEOUT = TimeUnit.SECONDS.toMillis(1);

    private final RestClient restClient;
    private final Request request;
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
    public ElasticsearchNodesSniffer(RestClient restClient) {
        this(restClient, DEFAULT_SNIFF_REQUEST_TIMEOUT, ElasticsearchNodesSniffer.Scheme.HTTP);
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
    public ElasticsearchNodesSniffer(RestClient restClient, long sniffRequestTimeoutMillis, Scheme scheme) {
        this.restClient = Objects.requireNonNull(restClient, "restClient cannot be null");
        if (sniffRequestTimeoutMillis < 0) {
            throw new IllegalArgumentException("sniffRequestTimeoutMillis must be greater than 0");
        }
        this.request = new Request("GET", "/_nodes/http");
        request.addParameter("timeout", sniffRequestTimeoutMillis + "ms");
        this.scheme = Objects.requireNonNull(scheme, "scheme cannot be null");
    }

    /**
     * Calls the elasticsearch nodes info api, parses the response and returns all the found http hosts
     */
    @Override
    public List<Node> sniff() throws IOException {
        Response response = restClient.performRequest(request);
        return readHosts(response.getEntity(), scheme, jsonFactory);
    }

    static List<Node> readHosts(HttpEntity entity, Scheme scheme, JsonFactory jsonFactory) throws IOException {
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
                            Node node = readNode(nodeId, parser, scheme);
                            if (node != null) {
                                nodes.add(node);
                            }
                        }
                    } else {
                        parser.skipChildren();
                    }
                }
            }
            return nodes;
        }
    }

    private static Node readNode(String nodeId, JsonParser parser, Scheme scheme) throws IOException {
        HttpHost publishedHost = null;
        /*
         * We sniff the bound hosts so we can look up the node based on any
         * address on which it is listening. This is useful in Elasticsearch's
         * test framework where we sometimes publish ipv6 addresses but the
         * tests contact the node on ipv4.
         */
        Set<HttpHost> boundHosts = new HashSet<>();
        String name = null;
        String version = null;
        /*
         * Multi-valued attributes come with key = `real_key.index` and we
         * unflip them after reading them because we can't rely on the order
         * that they arive.
         */
        final Map<String, String> protoAttributes = new HashMap<String, String>();

        boolean sawRoles = false;
        final Set<String> roles = new TreeSet<>();

        String fieldName = null;
        while (parser.nextToken() != JsonToken.END_OBJECT) {
            if (parser.getCurrentToken() == JsonToken.FIELD_NAME) {
                fieldName = parser.getCurrentName();
            } else if (parser.getCurrentToken() == JsonToken.START_OBJECT) {
                if ("http".equals(fieldName)) {
                    while (parser.nextToken() != JsonToken.END_OBJECT) {
                        if (parser.getCurrentToken() == JsonToken.VALUE_STRING && "publish_address".equals(parser.getCurrentName())) {
                            String address = parser.getValueAsString();
                            String host;
                            URI publishAddressAsURI;

                            // ES7 cname/ip:port format
                            if (address.contains("/")) {
                                String[] cnameAndURI = address.split("/", 2);
                                publishAddressAsURI = URI.create(scheme + "://" + cnameAndURI[1]);
                                host = cnameAndURI[0];
                            } else {
                                publishAddressAsURI = URI.create(scheme + "://" + address);
                                host = publishAddressAsURI.getHost();
                            }
                            publishedHost = new HttpHost(host, publishAddressAsURI.getPort(), publishAddressAsURI.getScheme());
                        } else if (parser.currentToken() == JsonToken.START_ARRAY && "bound_address".equals(parser.getCurrentName())) {
                            while (parser.nextToken() != JsonToken.END_ARRAY) {
                                URI boundAddressAsURI = URI.create(scheme + "://" + parser.getValueAsString());
                                boundHosts.add(
                                    new HttpHost(boundAddressAsURI.getHost(), boundAddressAsURI.getPort(), boundAddressAsURI.getScheme())
                                );
                            }
                        } else if (parser.getCurrentToken() == JsonToken.START_OBJECT) {
                            parser.skipChildren();
                        }
                    }
                } else if ("attributes".equals(fieldName)) {
                    while (parser.nextToken() != JsonToken.END_OBJECT) {
                        if (parser.getCurrentToken() == JsonToken.VALUE_STRING) {
                            String oldValue = protoAttributes.put(parser.getCurrentName(), parser.getValueAsString());
                            if (oldValue != null) {
                                throw new IOException("repeated attribute key [" + parser.getCurrentName() + "]");
                            }
                        } else {
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
                        roles.add(parser.getText());
                    }
                } else {
                    parser.skipChildren();
                }
            } else if (parser.currentToken().isScalarValue()) {
                if ("version".equals(fieldName)) {
                    version = parser.getText();
                } else if ("name".equals(fieldName)) {
                    name = parser.getText();
                }
            }
        }
        // http section is not present if http is not enabled on the node, ignore such nodes
        if (publishedHost == null) {
            logger.debug("skipping node [" + nodeId + "] with http disabled");
            return null;
        }

        Map<String, List<String>> realAttributes = new HashMap<>(protoAttributes.size());
        List<String> keys = new ArrayList<>(protoAttributes.keySet());
        for (String key : keys) {
            if (key.endsWith(".0")) {
                String realKey = key.substring(0, key.length() - 2);
                List<String> values = new ArrayList<>();
                int i = 0;
                while (true) {
                    String value = protoAttributes.remove(realKey + "." + i);
                    if (value == null) {
                        break;
                    }
                    values.add(value);
                    i++;
                }
                realAttributes.put(realKey, unmodifiableList(values));
            }
        }
        for (Map.Entry<String, String> entry : protoAttributes.entrySet()) {
            realAttributes.put(entry.getKey(), singletonList(entry.getValue()));
        }

        if (version.startsWith("2.")) {
            /*
             * 2.x doesn't send roles, instead we try to read them from
             * attributes.
             */
            boolean clientAttribute = v2RoleAttributeValue(realAttributes, "client", false);
            Boolean masterAttribute = v2RoleAttributeValue(realAttributes, "master", null);
            Boolean dataAttribute = v2RoleAttributeValue(realAttributes, "data", null);
            if ((masterAttribute == null && false == clientAttribute) || masterAttribute) {
                roles.add("master");
            }
            if ((dataAttribute == null && false == clientAttribute) || dataAttribute) {
                roles.add("data");
            }
        } else {
            assert sawRoles : "didn't see roles for [" + nodeId + "]";
        }
        assert boundHosts.contains(publishedHost) : "[" + nodeId + "] doesn't make sense! publishedHost should be in boundHosts";
        logger.trace("adding node [" + nodeId + "]");
        return new Node(publishedHost, boundHosts, name, version, new Roles(roles), unmodifiableMap(realAttributes));
    }

    /**
     * Returns {@code defaultValue} if the attribute didn't come back,
     * {@code true} or {@code false} if it did come back as
     * either of those, or throws an IOException if the attribute
     * came back in a strange way.
     */
    private static Boolean v2RoleAttributeValue(Map<String, List<String>> attributes, String name, Boolean defaultValue)
        throws IOException {
        List<String> valueList = attributes.remove(name);
        if (valueList == null) {
            return defaultValue;
        }
        if (valueList.size() != 1) {
            throw new IOException("expected only a single attribute value for [" + name + "] but got " + valueList);
        }
        switch (valueList.get(0)) {
            case "true":
                return true;
            case "false":
                return false;
            default:
                throw new IOException("expected [" + name + "] to be either [true] or [false] but was [" + valueList.get(0) + "]");
        }
    }

    public enum Scheme {
        HTTP("http"),
        HTTPS("https");

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
