/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.reindex.TransportReindexAction;
import org.elasticsearch.rest.root.MainRestPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;

/**
 * Verifies {@link ReindexPlugin#REINDEX_PIT_KEEP_ALIVE_SETTING} is applied when reindex opens a point-in-time on the local cluster.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0)
public class ReindexPitKeepAliveIT extends ESIntegTestCase {

    /** Captured keep-alive from the last open-PIT transport request observed on this node (inter-node traffic). */
    public static final AtomicReference<TimeValue> LAST_OPEN_PIT_KEEP_ALIVE = new AtomicReference<>();

    public static final class OpenPitKeepAliveCapturingPlugin extends Plugin implements NetworkPlugin {
        @Override
        public List<TransportInterceptor> getTransportInterceptors(
            NamedWriteableRegistry namedWriteableRegistry,
            ThreadContext threadContext
        ) {
            return List.of(new TransportInterceptor() {
                @Override
                public AsyncSender interceptSender(AsyncSender sender) {
                    return new AsyncSender() {
                        @Override
                        public <T extends TransportResponse> void sendRequest(
                            Transport.Connection connection,
                            String action,
                            TransportRequest request,
                            TransportRequestOptions options,
                            TransportResponseHandler<T> handler
                        ) {
                            if (TransportOpenPointInTimeAction.TYPE.name().equals(action)
                                && request instanceof OpenPointInTimeRequest openPit) {
                                LAST_OPEN_PIT_KEEP_ALIVE.set(openPit.keepAlive());
                            }
                            sender.sendRequest(connection, action, request, options, handler);
                        }
                    };
                }
            });
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ReindexPlugin.class, MainRestPlugin.class, OpenPitKeepAliveCapturingPlugin.class);
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(TransportReindexAction.REMOTE_CLUSTER_WHITELIST.getKey(), "*:*")
            .build();
    }

    @Override
    public void tearDown() throws Exception {
        try {
            assertAcked(
                clusterAdmin().prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .setPersistentSettings(
                        Settings.builder().putNull(ReindexPlugin.REINDEX_PIT_KEEP_ALIVE_SETTING.getKey()).build()
                    )
            );
        } finally {
            LAST_OPEN_PIT_KEEP_ALIVE.set(null);
            super.tearDown();
        }
    }

    public void testPersistentClusterSettingControlsLocalReindexOpenPitKeepAlive() throws Exception {
        assumeTrue("reindex PIT search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);

        final TimeValue configured = TimeValue.timeValueMinutes(16);
        assertAcked(
            clusterAdmin().prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .setPersistentSettings(
                    Settings.builder()
                        .put(ReindexPlugin.REINDEX_PIT_KEEP_ALIVE_SETTING.getKey(), configured.getStringRep())
                        .build()
                )
        );
        assertThat(
            clusterService().getClusterSettings().get(ReindexPlugin.REINDEX_PIT_KEEP_ALIVE_SETTING),
            equalTo(configured)
        );

        LAST_OPEN_PIT_KEEP_ALIVE.set(null);

        final String source = "source-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        final String dest = "dest-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        assertAcked(prepareCreate(source).get());
        assertAcked(prepareCreate(dest).get());
        final int numDocs = between(1, 20);
        indexRandom(true, source, numDocs);

        final ReindexRequest reindexRequest = new ReindexRequest().setSourceIndices(source).setDestIndex(dest);
        reindexRequest.setScroll(TimeValue.timeValueHours(3));

        BulkByScrollResponse reindexResponse = client().execute(ReindexAction.INSTANCE, reindexRequest).actionGet();
        assertThat(reindexResponse.getSearchFailures().size(), equalTo(0));
        assertThat(reindexResponse.getBulkFailures().size(), equalTo(0));

        assertBusy(() -> assertThat(LAST_OPEN_PIT_KEEP_ALIVE.get(), equalTo(configured)));
        assertHitCount(prepareSearch(dest).setSize(0), numDocs);
    }
}
