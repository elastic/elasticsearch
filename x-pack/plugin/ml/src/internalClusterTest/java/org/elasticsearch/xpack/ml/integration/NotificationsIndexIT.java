/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ml.notifications.NotificationsIndex;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

public class NotificationsIndexIT extends MlSingleNodeTestCase {

    @Override
    protected Settings nodeSettings() {
        Settings.Builder newSettings = Settings.builder();
        newSettings.put(super.nodeSettings());
        newSettings.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        newSettings.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        return newSettings.build();
    }

    public void testAliasCreated() throws Exception {
        // Auditing a notification should create the .ml-notifications-000002 index
        // and write alias
        createNotification(true);

        assertBusy(() -> {
            assertNotificationsIndexExists();
            assertNotificationsWriteAliasCreated();
        });
    }

    private void assertNotificationsIndexExists() {
        GetIndexResponse getIndexResponse = indicesAdmin().prepareGetIndex(TEST_REQUEST_TIMEOUT)
            .setIndices(NotificationsIndex.NOTIFICATIONS_INDEX)
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
            .get();
        assertThat(Arrays.asList(getIndexResponse.getIndices()), contains(NotificationsIndex.NOTIFICATIONS_INDEX));
    }

    private void assertNotificationsWriteAliasCreated() {
        Map<String, List<AliasMetadata>> aliases = indicesAdmin().prepareGetAliases(
            TimeValue.timeValueSeconds(10L),
            NotificationsIndex.NOTIFICATIONS_INDEX_WRITE_ALIAS
        ).setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN).get().getAliases();
        assertThat(aliases.size(), is(1));
        List<AliasMetadata> indexAliases = aliases.get(NotificationsIndex.NOTIFICATIONS_INDEX);
        assertNotNull(aliases.toString(), indexAliases);
        assertThat(indexAliases.size(), is(1));
        var writeAlias = indexAliases.get(0);
        assertThat(writeAlias.alias(), is(NotificationsIndex.NOTIFICATIONS_INDEX_WRITE_ALIAS));
        assertThat("notification write alias should be hidden but is not: " + aliases, writeAlias.isHidden(), is(true));
    }

    private void createNotification(boolean includeNodeInfo) {
        AnomalyDetectionAuditor auditor = new AnomalyDetectionAuditor(
            client(),
            getInstanceFromNode(ClusterService.class),
            TestIndexNameExpressionResolver.newInstance(),
            includeNodeInfo
        );
        auditor.info("whatever", "blah");
    }
}
