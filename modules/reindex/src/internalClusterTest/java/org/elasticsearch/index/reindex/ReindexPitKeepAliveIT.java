/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.reindex.ReindexSettings;
import org.elasticsearch.reindex.TransportReindexAction;
import org.elasticsearch.rest.root.MainRestPlugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;

/**
 * Verifies {@link ReindexSettings#REINDEX_PIT_KEEP_ALIVE_SETTING} is applied when reindex opens a point-in-time on the local cluster.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0)
public class ReindexPitKeepAliveIT extends ESIntegTestCase {

    /**
     * Captured keep-alive from the last {@link OpenPointInTimeRequest} executed on this JVM (local actions bypass transport handlers).
     */
    public static final AtomicReference<TimeValue> LAST_OPEN_PIT_KEEP_ALIVE = new AtomicReference<>();

    public static final class OpenPitKeepAliveCapturingPlugin extends Plugin implements ActionPlugin {
        @Override
        public List<ActionFilter> getActionFilters() {
            return List.of(new ActionFilter() {
                @Override
                public int order() {
                    return Integer.MIN_VALUE;
                }

                @Override
                public <Request extends ActionRequest, Response extends ActionResponse> void apply(
                    Task task,
                    String action,
                    Request request,
                    ActionListener<Response> listener,
                    ActionFilterChain<Request, Response> chain
                ) {
                    if (TransportOpenPointInTimeAction.TYPE.name().equals(action) && request instanceof OpenPointInTimeRequest openPit) {
                        LAST_OPEN_PIT_KEEP_ALIVE.set(openPit.keepAlive());
                    }
                    chain.proceed(task, action, request, listener);
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
    @After
    public void tearDown() throws Exception {
        try {
            assertAcked(
                clusterAdmin().prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .setPersistentSettings(Settings.builder().putNull(ReindexSettings.REINDEX_PIT_KEEP_ALIVE_SETTING.getKey()).build())
            );
        } finally {
            LAST_OPEN_PIT_KEEP_ALIVE.set(null);
            super.tearDown();
        }
    }

    /**
     * Asserts local reindex’s open-PIT uses {@link org.elasticsearch.reindex.ReindexSettings#REINDEX_PIT_KEEP_ALIVE_SETTING}
     * by updating the cluster setting, running a reindex task, and comparing to the value captured from
     * {@link org.elasticsearch.action.search.OpenPointInTimeRequest#keepAlive()}.
     */
    public void testClusterSettingControlsLocalReindexOpenPitKeepAlive() throws Exception {
        assumeTrue("reindex PIT search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);

        final TimeValue configured = TimeValue.timeValueMinutes(randomIntBetween(10, 20));
        assertAcked(
            clusterAdmin().prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .setPersistentSettings(
                    Settings.builder().put(ReindexSettings.REINDEX_PIT_KEEP_ALIVE_SETTING.getKey(), configured.getStringRep()).build()
                )
        );
        assertThat(clusterService().getClusterSettings().get(ReindexSettings.REINDEX_PIT_KEEP_ALIVE_SETTING), equalTo(configured));

        LAST_OPEN_PIT_KEEP_ALIVE.set(null);

        final String source = "source-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        final String dest = "dest-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        assertAcked(prepareCreate(source).get());
        assertAcked(prepareCreate(dest).get());
        final int numDocs = between(1, 20);
        indexRandom(true, source, numDocs);
        assertNoFailures(indicesAdmin().prepareRefresh(source).get());

        final ReindexRequest reindexRequest = new ReindexRequest().setSourceIndices(source).setDestIndex(dest);
        reindexRequest.setScroll(TimeValue.timeValueMinutes(randomIntBetween(0, 5)));

        BulkByScrollResponse reindexResponse = client().execute(ReindexAction.INSTANCE, reindexRequest).actionGet();
        assertThat(reindexResponse.getSearchFailures().size(), equalTo(0));
        assertThat(reindexResponse.getBulkFailures().size(), equalTo(0));
        assertThat(reindexResponse.getCreated(), equalTo((long) numDocs));

        assertThat(LAST_OPEN_PIT_KEEP_ALIVE.get(), equalTo(configured));
        assertBusy(() -> assertHitCount(prepareSearch(dest).setSize(0).setTrackTotalHits(true), numDocs));
    }
}
