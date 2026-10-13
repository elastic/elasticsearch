/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.enrich.LookupFromIndexService;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

public class LookupJoinRetryIT extends AbstractEsqlIntegTestCase {

    private static final String LOOKUP_INDEX = "languages_lookup";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }

    /**
     * Non-streaming LOOKUP JOIN routes its lookups the same way as ENRICH, so it can also reach a copy of the lookup
     * index that was just relocated away. The lookup must move on to another copy instead of failing the query.
     * The streaming operator does not retry, which is why it is disabled here.
     */
    public void testLookupJoinRetriesAnotherCopyWhenShardIsNotAvailable() {
        internalCluster().ensureAtLeastNumDataNodes(2);
        createLookupIndexOnEveryDataNode();
        updateClusterSettings(Settings.builder().put(EsqlPlugin.LOOKUP_JOIN_STREAMING.getKey(), false));
        AtomicBoolean failedOnce = new AtomicBoolean();
        for (var ts : internalCluster().getInstances(TransportService.class)) {
            ((MockTransportService) ts).addRequestHandlingBehavior(LookupFromIndexService.LOOKUP_ACTION_NAME, (h, r, channel, t) -> {
                if (failedOnce.compareAndSet(false, true)) {
                    channel.sendResponse(new RemoteTransportException("test", new IndexNotFoundException(LOOKUP_INDEX)));
                } else {
                    h.messageReceived(r, channel, t);
                }
            });
        }
        try {
            // ROW runs on the coordinator only, so no data-node retry on another copy of a source index can hide a failed lookup.
            try (var resp = run(syncEsqlQueryRequest("ROW language_code = 1 | LOOKUP JOIN " + LOOKUP_INDEX + " ON language_code"))) {
                assertThat(EsqlTestUtils.getValuesList(resp), equalTo(List.of(List.of(1, "english"))));
            }
            assertTrue("the first lookup should have been failed", failedOnce.get());
        } finally {
            for (var ts : internalCluster().getInstances(TransportService.class)) {
                ((MockTransportService) ts).clearAllRules();
            }
            updateClusterSettings(Settings.builder().putNull(EsqlPlugin.LOOKUP_JOIN_STREAMING.getKey()));
        }
    }

    private void createLookupIndexOnEveryDataNode() {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(LOOKUP_INDEX)
                .setSettings(
                    Settings.builder()
                        .put(IndexSettings.MODE.getKey(), IndexMode.LOOKUP)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, internalCluster().numDataNodes() - 1)
                )
                .setMapping("language_code", "type=integer", "language", "type=keyword")
        );
        prepareIndex(LOOKUP_INDEX).setSource("language_code", 1, "language", "english").get();
        refresh(LOOKUP_INDEX);
        ensureGreen(LOOKUP_INDEX);
    }
}
