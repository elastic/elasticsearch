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

package org.elasticsearch.ingest.common;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;
import static org.elasticsearch.ingest.ConfigurationUtils.readOptionalStringProperty;
import static org.elasticsearch.ingest.ConfigurationUtils.readStringProperty;

/**
 * Processor that adds new fields with their corresponding values based on the result of search request
 * against an elasticsearch cluster. It can use an external cluster using REST protocol.
 */
public final class SearchProcessor extends AbstractProcessor {

    private static Logger logger = ESLoggerFactory.getLogger("ingest.common");

    public static final String TYPE = "search";

    private final RestClient client;
    private final String endpoint;
    private final String query;
    private final String path;
    private final String targetField;

    /**
     * Create a SearchProcessor instance
     * @param tag Tag
     * @param client REST Client
     * @param index index (null if not set)
     * @param type type (null if not set). If set, index must be set.
     * @param query JSON query. Defaults to { "query" : { "match_all": {} } }
     * @param path defines a path to apply on the response to extract the object to be merged in the target field.
     *             Defaults to hits.hits[0]._source
     * @param targetField target field. If it does not exist, it will be created. If it exists as an object, the response
     *               content will be merged. If it exists as a simple value like "foo": "bar", "foo" will become an object
     *               containing the response.
     *               If not set, the result will be merged on the root level.
     */
    SearchProcessor(String tag, RestClient client, String index, String type, String query, String path, String targetField)  {
        super(tag);
        this.client = client;
        this.query = query;
        this.path = path;
        this.targetField = targetField;

        String localEndpoint = "/";
        if (index != null) {
            localEndpoint += index + "/";
        }
        if (type != null) {
            localEndpoint += type + "/";
        }
        localEndpoint += "_search";
        this.endpoint = localEndpoint;
    }

    @Override
    public void execute(IngestDocument document) {

        try {
            StringEntity entity = new StringEntity(query, ContentType.APPLICATION_JSON);

            Response response = client.performRequest("GET", endpoint, Collections.singletonMap("size", "1"), entity);
            XContentType xContentType = XContentType.JSON;
            XContentParser parser = xContentType.xContent().createParser(response.getEntity().getContent());
            Map<String, Object> map = parser.map();
            int total = (int) XContentMapValues.extractValue("hits.total", map);

            logger.trace("Got {}", map);

            if (total > 0) {
                Object oSource = XContentMapValues.extractValue(path, map);

                if (oSource == null) {
                    // We found no document
                    logger.debug("No document found for [{}] with path [{}]. Got {}", query, path, map);
                } else {
                    // We add what we found to our ingest document
                    logger.debug("Enriching document with [{}]", oSource);

                    // If source is an array, we get the first one
                    if (oSource instanceof List) {
                        List source = (List) oSource;
                        if (!source.isEmpty()) {
                            oSource = source.get(0);
                        }
                    }

                    logger.debug("oSource is [{}]", oSource);

                    if (targetField != null) {
                        if (document.hasField(targetField)) {
                            // The target field already exists.
                            Object oTarget = document.getFieldValue(targetField, Object.class);

                            logger.debug("oTarget is [{}]", oTarget);

                            if (oTarget instanceof Map && oSource instanceof Map) {
                                // If it's an object (Map), we can merge the new values
                                // and if we have an object in oSource, like { "foo": "bar" }
                                // We merge objects
                                Map<String, Object> source = (Map<String, Object>) oSource;
                                for (Map.Entry<String, Object> entry : source.entrySet()) {
                                    document.setFieldValue(targetField + "." + entry.getKey(), entry.getValue());
                                }
                            }
                            else {
                                // We are going to overwrite any existing value
                                document.setFieldValue(targetField, oSource);
                            }
                        } else {
                            logger.debug("writing [{}] at [{}]", oSource, targetField);
                            document.setFieldValue(targetField, oSource);
                        }
                    } else {
                        // We merge the result directly on the top level
                        if (oSource instanceof Map) {
                            // If it's an object (Map), we can merge the new values
                            // and if we have an object in oSource, like { "foo": "bar" }
                            // We merge objects
                            Map<String, Object> source = (Map<String, Object>) oSource;
                            for (Map.Entry<String, Object> entry : source.entrySet()) {
                                document.setFieldValue(entry.getKey(), entry.getValue());
                            }
                        } else {
                            throw new ElasticsearchException("We can not merge top level values without a field name");
                        }
                    }
                }
            } else {
                // No hit found
                logger.debug("No hits found for query [{}]. Got {}", query, map);
            }

        } catch (IOException e) {
            throw new ElasticsearchException("Error while executing search", e);
        }

        logger.debug("Document generated [{}]", document);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    /**
     * The Factory helps to create a SearchProcessor instance.
     * It supports the following settings:
     * <ul>
     *     <li>host: the elasticsearch server we want to run the search requests on, defaults to "http://127.0.0.1:9200"
     *     <li>username: elasticsearch username (for plain text auth)
     *     <li>password: elasticsearch password (for plain text auth)
     *     <li>index: index we want to run the query on
     *     <li>type: type we want to run the query on
     *     <li>query: elasticsearch query to run, defaults to { "query" : { "match_all": {} } }
     *     <li>path: defines a path to apply on the response to extract the object to be merged in the target field.
     *         Defaults to hits.hits.0._source
     *     <li>target_field: destination field. Defaults to "target".
     * </ul>
     */
    public static final class Factory implements Closeable, Processor.Factory {

        public static final String DEFAULT_HOST = "http://127.0.0.1:9200";
        public static final String DEFAULT_QUERY = "{ \"query\" : { \"match_all\": {} } }";
        public static final String DEFAULT_PATH = "hits.hits._source";

        protected final Map<String, RestClient> clients;

        public Factory() {
            clients = new HashMap<>();
        }

        @Override
        public SearchProcessor create(Map<String, Processor.Factory> registry,
                                      String processorTag,
                                      Map<String, Object> config) throws Exception {
            // TODO we should default to the current node http layer if available
            String host = readStringProperty(TYPE, processorTag, config, "host", DEFAULT_HOST);
            String username = readOptionalStringProperty(TYPE, processorTag, config, "username");
            // TODO probably a bad idea to have that in clear
            String password = readOptionalStringProperty(TYPE, processorTag, config, "password");
            String index = readOptionalStringProperty(TYPE, processorTag, config, "index");
            String type = readOptionalStringProperty(TYPE, processorTag, config, "type");
            String query = readStringProperty(TYPE, processorTag, config, "query", DEFAULT_QUERY);
            String path = readStringProperty(TYPE, processorTag, config, "path", DEFAULT_PATH);
            String targetField = readOptionalStringProperty(TYPE, processorTag, config, "target_field");

            if (index != null && index.isEmpty()) {
                throw newConfigurationException(TYPE, processorTag, "index", "when set can't be empty");
            }

            if (type != null && type.isEmpty()) {
                throw newConfigurationException(TYPE, processorTag, "type", "when set can't be empty");
            }

            // If type is defined, index must be set
            if (type != null && index == null) {
                throw newConfigurationException(TYPE, processorTag, "index/type", ": if [type] is set, [index] must be set as well");
            }

            // We reuse the same connection to the same server instead of building a new instance every time
            RestClient client = clients.get(host);

            if (client == null) {
                logger.debug("creating REST client for host [{}]", host);
                HttpHost httpHost = HttpHost.create(host);

                RestClientBuilder builder = RestClient.builder(httpHost);


                if (username != null) {
                    CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
                    builder.setHttpClientConfigCallback(httpClientBuilder ->
                        httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
                }

                client = builder.build();
                clients.put(host, client);
            }

            return new SearchProcessor(processorTag, client, index, type, query, path, targetField);
        }

        @Override
        public void close() throws IOException {
            for (Map.Entry<String, RestClient> clientEntry : clients.entrySet()) {
                logger.debug("closing REST client for host [{}]", clientEntry.getKey());
                clientEntry.getValue().close();
            }
        }
    }
}
