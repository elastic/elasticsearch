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

package org.elasticsearch.index.reindex;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.UncategorizedExecutionException;
import org.elasticsearch.index.reindex.remote.RemoteInfo;

import java.io.IOException;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.endsWith;

/**
 * Attempts to reindex-from-remote from a port that never accepts connections, triggering a failure on connection pool's thread pool.
 */
public class ReindexFromRemoteBrokenRemoteTests extends ReindexTestCase {
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal));
        // Whitelist reindexing from localhost
        settings.put(TransportReindexAction.REMOTE_CLUSTER_WHITELIST.getKey(), "127.0.0.1:*");
        return settings.build();
    }

    public void testRemoteDown() throws IOException {
        RemoteInfo remoteInfo = new RemoteInfo("http", "127.0.0.1", 0, new BytesArray("{\"match_all\":{}}"),
                null, null, emptyMap(), RemoteInfo.DEFAULT_SOCKET_TIMEOUT, RemoteInfo.DEFAULT_CONNECT_TIMEOUT);
        ReindexRequestBuilder request = ReindexAction.INSTANCE.newRequestBuilder(client()).source("source").destination("dest")
                .setRemoteInfo(remoteInfo);
        Throwable e = expectThrows(UncategorizedExecutionException.class, () -> request.get());
        assertThat(e.getCause().getMessage(), endsWith("Connection refused"));
    }
}
