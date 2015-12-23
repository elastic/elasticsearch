/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.cleaner.http;

import com.squareup.okhttp.mockwebserver.Dispatcher;
import com.squareup.okhttp.mockwebserver.MockResponse;
import com.squareup.okhttp.mockwebserver.MockWebServer;
import com.squareup.okhttp.mockwebserver.RecordedRequest;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.marvel.agent.exporter.Exporter;
import org.elasticsearch.marvel.agent.exporter.http.HttpExporter;
import org.elasticsearch.marvel.cleaner.AbstractIndicesCleanerTestCase;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.BindException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

public class HttpIndicesCleanerTests extends AbstractIndicesCleanerTestCase {

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
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("marvel.agent.exporters._http.type", HttpExporter.TYPE)
                .put("marvel.agent.exporters._http.host", webServer.getHostName() + ":" + webServer.getPort())
                .put("marvel.agent.exporters._http.connection.keep_alive", false)
                .build();
    }

    @Override
    protected void createIndex(String name, DateTime creationDate) {
        dispatcher.addIndex(name, creationDate.getMillis());
    }

    @Override
    protected void assertIndicesCount(int count) throws Exception {
        assertThat(dispatcher.indices.size(), equalTo(count));
    }

    class MockServerDispatcher extends Dispatcher {

        private Map<String, Long> indices = ConcurrentCollections.newConcurrentMap();

        @Override
        public MockResponse dispatch(RecordedRequest request) throws InterruptedException {
            final String[] paths = Strings.splitStringToArray(request.getPath(), '/');
            switch (request.getMethod()) {
                case "GET":
                    if ((paths != null) && (paths.length == 3) && "_settings".equals(paths[1])) {
                        try {
                            // Builds a Get Settings response
                            XContentBuilder builder = jsonBuilder().startObject();
                            for (Map.Entry<String, Long> index : indices.entrySet()) {
                                builder.startObject(index.getKey());
                                builder.startObject("settings");
                                Settings settings = Settings.builder().put(IndexMetaData.SETTING_CREATION_DATE, index.getValue()).build();
                                settings.toXContent(builder, ToXContent.EMPTY_PARAMS);
                                builder.endObject();
                                builder.endObject();
                            }
                            builder.endObject();
                            return new MockResponse().setResponseCode(200).setBody(builder.string());
                        } catch (IOException e) {
                            return new MockResponse().setResponseCode(500).setBody(e.getMessage());
                        }
                    }
                    break;
                case "DELETE":
                    if ((paths != null) && (paths.length == 1)) {
                        String[] deletions = Strings.splitStringByCommaToArray(paths[0]);
                        if (deletions != null && deletions.length > 0) {
                            for (String deletion : deletions) {
                                if (indices.containsKey(deletion)) {
                                    indices.remove(deletion);
                                } else {
                                    return new MockResponse().setResponseCode(404);
                                }
                            }
                            return new MockResponse().setResponseCode(200);
                        }
                    }
                    break;
            }
            return new MockResponse().setResponseCode(404).setBody("unsupported request: " + request.getRequestLine());
        }

        public void addIndex(String name, long creation) {
            indices.put(name, creation);
        }
    }
}