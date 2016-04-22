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
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.marvel.agent.exporter.AbstractExporterTemplateTestCase;
import org.elasticsearch.marvel.agent.exporter.Exporter;
import org.junit.After;
import org.junit.Before;

import java.net.BindException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
    protected void deleteTemplates() throws Exception {
        dispatcher.templates.clear();
    }

    @Override
    protected void putTemplate(String name) throws Exception {
        dispatcher.templates.put(name, generateTemplateSource(name));
    }

    @Override
    protected void assertTemplateExist(String name) throws Exception {
        assertThat("failed to find a template matching [" + name + "]", dispatcher.templates.containsKey(name), is(true));
    }

    @Override
    protected void assertTemplateNotUpdated(String name) throws Exception {
        // Checks that no PUT Template request has been made
        assertThat(dispatcher.hasRequest("PUT", "/_template/" + name), is(false));

        // Checks that the current template exists
        assertThat(dispatcher.templates.containsKey(name), is(true));
    }

    @Override
    protected void awaitIndexExists(String index) throws Exception {
        assertBusy(() -> assertThat("could not find index " + index, dispatcher.hasIndex(index), is(true)), 10, TimeUnit.SECONDS);
    }

    class MockServerDispatcher extends Dispatcher {

        private final MockResponse NOT_FOUND = newResponse(404, "");

        private final Set<String> requests = new HashSet<>();
        private final Map<String, BytesReference> templates = ConcurrentCollections.newConcurrentMap();
        private final Set<String> indices = ConcurrentCollections.newConcurrentSet();

        @Override
        public MockResponse dispatch(RecordedRequest request) throws InterruptedException {
            final String requestLine = request.getRequestLine();
            requests.add(requestLine);

            switch (requestLine) {
                // Cluster version
                case "GET / HTTP/1.1":
                    return newResponse(200, "{\"version\": {\"number\": \"" + Version.CURRENT.toString() + "\"}}");
                // Bulk
                case "POST /_bulk HTTP/1.1":
                    // Parse the bulk request and extract all index names
                    try {
                        BulkRequest bulk = new BulkRequest();
                        byte[] source = request.getBody().readByteArray();
                        bulk.add(source, 0, source.length);
                        for (ActionRequest docRequest : bulk.requests()) {
                            if (docRequest instanceof IndexRequest) {
                                indices.add(((IndexRequest) docRequest).index());
                            }
                        }
                    } catch (Exception e) {
                        return newResponse(500, e.getMessage());
                    }
                    return newResponse(200, "{\"errors\": false, \"msg\": \"successful bulk request\"}");
                default:
                    String[] paths = Strings.splitStringToArray(request.getPath(), '/');

                    // Templates
                    if ((paths != null) && (paths.length > 0) && ("_template".equals(paths[0]))) {
                        String templateName = paths[1];
                        boolean templateExist = templates.containsKey(templateName);

                        if ("GET".equals(request.getMethod())) {
                            return templateExist ? newResponse(200, templates.get(templateName).toUtf8()) : NOT_FOUND;
                        }
                        if ("PUT".equals(request.getMethod())) {
                            templates.put(templateName, new BytesArray(request.getBody().readByteArray()));
                            return templateExist ? newResponse(200, "updated") : newResponse(201, "created");
                        }
                    }
                    break;
            }
            return newResponse(500, "MockServerDispatcher does not support: " + request.getRequestLine());
        }

        MockResponse newResponse(int code, String body) {
            return new MockResponse().setResponseCode(code).setBody(body);
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

        boolean hasIndex(String index) {
            return indices.contains(index);
        }
    }
}
