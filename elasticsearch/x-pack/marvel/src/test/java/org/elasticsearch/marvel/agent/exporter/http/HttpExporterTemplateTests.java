/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter.http;

import com.squareup.okhttp.mockwebserver.Dispatcher;
import com.squareup.okhttp.mockwebserver.MockResponse;
import com.squareup.okhttp.mockwebserver.MockWebServer;
import com.squareup.okhttp.mockwebserver.RecordedRequest;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.marvel.agent.exporter.AbstractExporterTemplateTestCase;
import org.elasticsearch.marvel.agent.exporter.Exporter;
import org.elasticsearch.marvel.agent.exporter.MarvelTemplateUtils;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.BindException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;

public class HttpExporterTemplateTests extends AbstractExporterTemplateTestCase {

    private MockWebServer webServer;
    private MockServerDispatcher dispatcher;

    @Before
    public void startWebServer() throws Exception {
        for (int webPort = 9250; webPort < 9300; webPort++) {
            try {
                webServer = new MockWebServer();
                dispatcher = new MockServerDispatcher();
                webServer.setDispatcher(dispatcher);
                webServer.start(webPort);
                return;
            } catch (BindException be) {
                logger.warn("port [{}] was already in use trying next port", webPort);
            }
        }
        throw new ElasticsearchException("unable to find open port between 9200 and 9300");
    }

    @After
    public void stopWebServer() throws Exception {
        webServer.shutdown();
    }

    @Override
    protected Settings exporterSettings() {
        return Settings.builder()
                .put("type", "http")
                .put("host", webServer.getHostName() + ":" + webServer.getPort())
                .put("connection.keep_alive", false)
                .put(Exporter.INDEX_NAME_TIME_FORMAT_SETTING, "YYYY")
                .build();
    }

    @Override
    protected void deleteTemplate() {
        dispatcher.setTemplate(null);
    }

    @Override
    protected void putTemplate(String version) throws Exception {
        dispatcher.setTemplate(generateTemplateSource(version).toBytes());
    }

    @Override
    protected void createMarvelIndex(String index) throws Exception {
        dispatcher.addIndex(index);
    }

    @Override
    protected void assertTemplateUpdated(Version version) {
        // Checks that a PUT Template request has been made
        assertThat(dispatcher.hasRequest("PUT", "/_template/" + MarvelTemplateUtils.INDEX_TEMPLATE_NAME), is(true));

        // Checks that the current template has the expected version
        assertThat(MarvelTemplateUtils.parseTemplateVersion(dispatcher.getTemplate()), equalTo(version));
    }

    @Override
    protected void assertTemplateNotUpdated(Version version) throws Exception {
        // Checks that no PUT Template request has been made
        assertThat(dispatcher.hasRequest("PUT", "/_template/" + MarvelTemplateUtils.INDEX_TEMPLATE_NAME), is(false));

        // Checks that the current template has the expected version
        assertThat(MarvelTemplateUtils.parseTemplateVersion(dispatcher.getTemplate()), equalTo(version));
    }

    @Override
    protected void assertIndicesNotCreated() throws Exception {
        // Checks that no Bulk request has been made
        assertThat(dispatcher.hasRequest("POST", "/_bulk"), is(false));
        assertThat(dispatcher.mappings.size(), equalTo(0));
    }

    @Override
    protected void assertMappingsUpdated(String... indices) throws Exception {
        // Load the mappings of the old template
        Set<String> oldMappings = new PutIndexTemplateRequest().source(generateTemplateSource(null)).mappings().keySet();

        // Load the mappings of the latest template
        Set<String> newMappings = new PutIndexTemplateRequest().source(generateTemplateSource(null)).mappings().keySet();
        newMappings.removeAll(oldMappings);

        for (String index : indices) {
            for (String mapping : newMappings) {
                // Checks that a PUT Mapping request has been made for every type that was not in the old template
                assertThat(dispatcher.hasRequest("PUT", "/" + index + "/_mapping/" + mapping), equalTo(true));
            }
        }
    }

    @Override
    protected void assertMappingsNotUpdated(String... indices) throws Exception {
        for (String index : indices) {
            // Checks that no PUT Template request has been made
            assertThat(dispatcher.hasRequest("PUT", "/" + index + "/_mapping/"), is(false));
        }
    }

    class MockServerDispatcher extends Dispatcher {

        private final MockResponse OK = newResponse(200, "");
        private final MockResponse NOT_FOUND = newResponse(404, "");

        private final Set<String> requests = new HashSet<>();

        private final Map<String, Set<String>> mappings = new HashMap<>();
        private byte[] template;

        @Override
        public MockResponse dispatch(RecordedRequest request) throws InterruptedException {
            synchronized (this) {
                final String requestLine = request.getRequestLine();
                requests.add(requestLine);

                switch (requestLine) {
                    // Cluster version
                    case "GET / HTTP/1.1":
                        return newResponse(200, "{\"version\": {\"number\": \"" + Version.CURRENT.number() + "\"}}");

                    // Template
                    case "GET /_template/.marvel-es HTTP/1.1":
                        return (template == null) ? NOT_FOUND : newResponse(200, new BytesArray(template).toUtf8());

                    case "PUT /_template/.marvel-es HTTP/1.1":
                        this.template = request.getBody().readByteArray();
                        return OK;

                    // Bulk
                    case "POST /_bulk HTTP/1.1":

                        return OK;
                    default:
                        String[] paths = Strings.splitStringToArray(request.getPath(), '/');

                        // Index Mappings
                        if ((paths != null) && (paths.length > 0) && ("_mapping".equals(paths[1]))) {

                            if (!mappings.containsKey(paths[0])) {
                                // Index does not exist
                                return NOT_FOUND;
                            }

                            // Get index mappings
                            if ("GET".equals(request.getMethod())) {
                                try {
                                    // Builds a fake mapping response
                                    XContentBuilder builder = jsonBuilder().startObject().startObject(paths[0]).startObject("mappings");
                                    for (String type : mappings.get(paths[0])) {
                                        builder.startObject(type).endObject();
                                    }
                                    builder.endObject().endObject().endObject();
                                    return newResponse(200, builder.bytes().toUtf8());
                                } catch (IOException e) {
                                    return newResponse(500, e.getMessage());
                                }

                                // Put index mapping
                            } else if ("PUT".equals(request.getMethod()) && paths.length > 2) {
                                Set<String> types = mappings.get(paths[0]);
                                if (types == null) {
                                    types = new HashSet<>();
                                }
                                types.add(paths[2]);
                                return OK;
                            }
                        }
                        break;
                }

                return newResponse(500, "MockServerDispatcher does not support: " + request.getRequestLine());
            }
        }

        MockResponse newResponse(int code, String body) {
            return new MockResponse().setResponseCode(code).setBody(body);
        }

        void setTemplate(byte[] template) {
            synchronized (this) {
                this.template = template;
            }
        }

        byte[] getTemplate() {
            return template;
        }

        void addIndex(String index) {
            synchronized (this) {
                if (template != null) {
                    // Simulate the use of the index template when creating an index
                    mappings.put(index, new HashSet<>(new PutIndexTemplateRequest().source(template).mappings().keySet()));
                } else {
                    mappings.put(index, null);
                }
            }
        }

        int countRequests(String method, String path) {
            int count = 0;
            for (String request : requests) {
                if (request.startsWith(method + " " + path)) {
                    count += 1;
                }
            }
            return count;
        }

        boolean hasRequest(String method, String path) {
            return countRequests(method, path) > 0;
        }
    }
}
