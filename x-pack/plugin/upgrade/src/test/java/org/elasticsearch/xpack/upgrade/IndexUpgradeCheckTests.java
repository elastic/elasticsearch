/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.upgrade;

import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.protocol.xpack.migration.UpgradeActionRequired;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;

import java.io.IOException;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexUpgradeCheckTests extends ESTestCase {

    private Client client;

    @Before
    public void setupClient() {
        client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(client.settings()).thenReturn(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
    }

    public void testWatchesIndexUpgradeCheck() throws Exception {
        IndexUpgradeCheck check = Upgrade.getWatchesIndexUpgradeCheckFactory(Settings.EMPTY).apply(client, null);
        assertThat(check.getName(), equalTo("watches"));

        IndexMetaData goodKibanaIndex = newTestIndexMeta(".kibana", Settings.EMPTY);
        assertThat(check.actionRequired(goodKibanaIndex), equalTo(UpgradeActionRequired.NOT_APPLICABLE));

        IndexMetaData watcherIndex = newTestIndexMeta(".watches", Settings.EMPTY);
        assertThat(check.actionRequired(watcherIndex), equalTo(UpgradeActionRequired.UPGRADE));

        IndexMetaData watcherIndexWithAlias = newTestIndexMeta("my_watches", ".watches", Settings.EMPTY);
        assertThat(check.actionRequired(watcherIndexWithAlias), equalTo(UpgradeActionRequired.UPGRADE));

        IndexMetaData watcherIndexWithAliasUpgraded = newTestIndexMeta("my_watches", ".watches",
                Settings.builder().put(IndexMetaData.INDEX_FORMAT_SETTING.getKey(), "6").build());
        assertThat(check.actionRequired(watcherIndexWithAliasUpgraded), equalTo(UpgradeActionRequired.UP_TO_DATE));
    }

    public void testTriggeredWatchesIndexUpgradeCheck() throws Exception {
        IndexUpgradeCheck check = Upgrade.getTriggeredWatchesIndexUpgradeCheckFactory(Settings.EMPTY).apply(client, null);
        assertThat(check.getName(), equalTo("triggered-watches"));

        IndexMetaData goodKibanaIndex = newTestIndexMeta(".kibana", Settings.EMPTY);
        assertThat(check.actionRequired(goodKibanaIndex), equalTo(UpgradeActionRequired.NOT_APPLICABLE));

        IndexMetaData watcherIndex = newTestIndexMeta(".triggered_watches", Settings.EMPTY);
        assertThat(check.actionRequired(watcherIndex), equalTo(UpgradeActionRequired.UPGRADE));

        IndexMetaData watcherIndexWithAlias = newTestIndexMeta("my_triggered_watches", ".triggered_watches", Settings.EMPTY);
        assertThat(check.actionRequired(watcherIndexWithAlias), equalTo(UpgradeActionRequired.UPGRADE));

        IndexMetaData watcherIndexWithAliasUpgraded = newTestIndexMeta("my_triggered_watches", ".triggered_watches",
                Settings.builder().put(IndexMetaData.INDEX_FORMAT_SETTING.getKey(), "6").build());
        assertThat(check.actionRequired(watcherIndexWithAliasUpgraded), equalTo(UpgradeActionRequired.UP_TO_DATE));
    }

    public void testSecurityIndexUpgradeCheck() throws Exception{
        IndexUpgradeCheck check = Upgrade.getSecurityUpgradeCheckFactory(Settings.EMPTY).apply(client, null);
        assertThat(check.getName(), equalTo("security"));

        IndexMetaData securityIndex = newTestIndexMeta(".security", Settings.EMPTY);
        assertThat(check.actionRequired(securityIndex), equalTo(UpgradeActionRequired.UPGRADE));
    }

    public static IndexMetaData newTestIndexMeta(String name, String alias, Settings indexSettings) throws IOException {
        Settings build = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_CREATION_DATE, 1)
                .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                .put(IndexMetaData.SETTING_VERSION_UPGRADED, Version.V_5_0_0_beta1)
                .put(indexSettings)
                .build();
        IndexMetaData.Builder builder = IndexMetaData.builder(name).settings(build);
        if (alias != null) {
            // Create alias
            builder.putAlias(AliasMetaData.newAliasMetaDataBuilder(alias).build());
        }
        return builder.build();
    }

    public static IndexMetaData newTestIndexMeta(String name, Settings indexSettings) throws IOException {
        return newTestIndexMeta(name, null, indexSettings);
    }

}
