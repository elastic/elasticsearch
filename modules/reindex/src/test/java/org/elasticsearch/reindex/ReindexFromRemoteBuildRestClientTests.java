/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reindex;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilderTestCase;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.reindex.RemoteInfo;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.synchronizedList;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;

public class ReindexFromRemoteBuildRestClientTests extends RestClientBuilderTestCase {

    private final BytesReference matchAll = new BytesArray(new MatchAllQueryBuilder().toString());

    public void testBuildRestClient() throws Exception {
        for (final String path : new String[] { "", null, "/", "path" }) {
            RemoteInfo remoteInfo = new RemoteInfo(
                "https",
                "localhost",
                9200,
                path,
                matchAll,
                null,
                null,
                emptyMap(),
                RemoteInfo.DEFAULT_SOCKET_TIMEOUT,
                RemoteInfo.DEFAULT_CONNECT_TIMEOUT
            );
            long taskId = randomLong();
            List<Thread> threads = synchronizedList(new ArrayList<>());
            RestClient client = Reindexer.buildRestClient(remoteInfo, sslConfig(), taskId, threads);
            try {
                assertBusy(() -> assertThat(threads, hasSize(2)));
                int i = 0;
                for (Thread thread : threads) {
                    assertEquals("es-client-" + taskId + "-" + i, thread.getName());
                    i++;
                }
            } finally {
                client.close();
            }
        }
    }

    public void testHeaders() throws Exception {
        Map<String, String> headers = new HashMap<>();
        int numHeaders = randomIntBetween(1, 5);
        for (int i = 0; i < numHeaders; ++i) {
            headers.put("header" + i, Integer.toString(i));
        }
        RemoteInfo remoteInfo = new RemoteInfo(
            "https",
            "localhost",
            9200,
            null,
            matchAll,
            null,
            null,
            headers,
            RemoteInfo.DEFAULT_SOCKET_TIMEOUT,
            RemoteInfo.DEFAULT_CONNECT_TIMEOUT
        );
        long taskId = randomLong();
        List<Thread> threads = synchronizedList(new ArrayList<>());
        RestClient client = Reindexer.buildRestClient(remoteInfo, sslConfig(), taskId, threads);
        try {
            assertHeaders(client, headers);
        } finally {
            client.close();
        }
    }

    private ReindexSslConfig sslConfig() {
        final Environment environment = TestEnvironment.newEnvironment(Settings.builder().put("path.home", createTempDir()).build());
        final ResourceWatcherService resourceWatcher = mock(ResourceWatcherService.class);
        return new ReindexSslConfig(environment.settings(), environment, resourceWatcher);
    }

}
