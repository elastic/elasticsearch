/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.upgrade;


import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.protocol.xpack.migration.UpgradeActionRequired;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;

import static org.hamcrest.core.IsEqual.equalTo;

public class IndexUpgradeServiceTests extends ESTestCase {

    private IndexUpgradeCheck upgradeBarCheck = new IndexUpgradeCheck("upgrade_bar",
            (Function<IndexMetaData, UpgradeActionRequired>) indexMetaData -> {
                if ("bar".equals(indexMetaData.getSettings().get("test.setting"))) {
                    return UpgradeActionRequired.UPGRADE;
                } else {
                    return UpgradeActionRequired.NOT_APPLICABLE;
                }
            }, null, null, null, null);

    private IndexUpgradeCheck reindexFooCheck = new IndexUpgradeCheck("reindex_foo",
            (Function<IndexMetaData, UpgradeActionRequired>) indexMetaData -> {
                if ("foo".equals(indexMetaData.getSettings().get("test.setting"))) {
                    return UpgradeActionRequired.REINDEX;
                } else {
                    return UpgradeActionRequired.NOT_APPLICABLE;
                }
            }, null, null, null, null);

    private IndexUpgradeCheck everythingIsFineCheck = new IndexUpgradeCheck("everything_is_fine",
            indexMetaData -> UpgradeActionRequired.UP_TO_DATE, null, null, null, null);

    private IndexUpgradeCheck unreachableCheck = new IndexUpgradeCheck("unreachable",
            (Function<IndexMetaData, UpgradeActionRequired>) indexMetaData -> {
                fail("Unreachable check is called");
                return null;
            }, null, null, null, null);

    public void testIndexUpgradeServiceMultipleCheck() throws Exception {
        IndexUpgradeService service;
        if (randomBoolean()) {
            service = new IndexUpgradeService(Arrays.asList(
                    upgradeBarCheck,
                    reindexFooCheck,
                    everythingIsFineCheck,
                    unreachableCheck // This one should never be called
            ));
        } else {
            service = new IndexUpgradeService(Arrays.asList(
                    reindexFooCheck,
                    upgradeBarCheck,
                    everythingIsFineCheck,
                    unreachableCheck // This one should never be called
            ));
        }

        IndexMetaData fooIndex = newTestIndexMeta("bar", Settings.builder().put("test.setting", "bar").build());
        IndexMetaData barIndex = newTestIndexMeta("foo", Settings.builder().put("test.setting", "foo").build());
        IndexMetaData bazIndex = newTestIndexMeta("baz", Settings.EMPTY);

        ClusterState clusterState = mockClusterState(fooIndex, barIndex, bazIndex);

        Map<String, UpgradeActionRequired> result = service.upgradeInfo(new String[]{"bar", "foo", "baz"},
                IndicesOptions.lenientExpandOpen(), clusterState);

        assertThat(result.size(), equalTo(2));
        assertThat(result.get("bar"), equalTo(UpgradeActionRequired.UPGRADE));
        assertThat(result.get("foo"), equalTo(UpgradeActionRequired.REINDEX));

        result = service.upgradeInfo(new String[]{"b*"}, IndicesOptions.lenientExpandOpen(), clusterState);

        assertThat(result.size(), equalTo(1));
        assertThat(result.get("bar"), equalTo(UpgradeActionRequired.UPGRADE));
    }


    public void testNoMatchingChecks() throws Exception {
        IndexUpgradeService service = new IndexUpgradeService(Arrays.asList(
                upgradeBarCheck,
                reindexFooCheck
        ));

        IndexMetaData fooIndex = newTestIndexMeta("bar", Settings.builder().put("test.setting", "bar").build());
        IndexMetaData barIndex = newTestIndexMeta("foo", Settings.builder().put("test.setting", "foo").build());
        IndexMetaData bazIndex = newTestIndexMeta("baz", Settings.EMPTY);

        ClusterState clusterState = mockClusterState(fooIndex, barIndex, bazIndex);

        Map<String, UpgradeActionRequired> result = service.upgradeInfo(new String[]{"bar", "foo", "baz"},
                IndicesOptions.lenientExpandOpen(), clusterState);

        assertThat(result.size(), equalTo(2));
        assertThat(result.get("bar"), equalTo(UpgradeActionRequired.UPGRADE));
        assertThat(result.get("foo"), equalTo(UpgradeActionRequired.REINDEX));
    }

    public void testEarlierChecksWin() throws Exception {
        IndexUpgradeService service = new IndexUpgradeService(Arrays.asList(
                everythingIsFineCheck,
                upgradeBarCheck,
                reindexFooCheck
        ));

        IndexMetaData fooIndex = newTestIndexMeta("bar", Settings.builder().put("test.setting", "bar").build());
        IndexMetaData barIndex = newTestIndexMeta("foo", Settings.builder().put("test.setting", "foo").build());
        IndexMetaData bazIndex = newTestIndexMeta("baz", Settings.EMPTY);

        ClusterState clusterState = mockClusterState(fooIndex, barIndex, bazIndex);

        Map<String, UpgradeActionRequired> result = service.upgradeInfo(new String[]{"bar", "foo", "baz"},
                IndicesOptions.lenientExpandOpen(), clusterState);

        assertThat(result.size(), equalTo(0)); // everything as the first checker should indicate that everything is fine
    }

    public void testGenericTest() throws Exception {
        IndexUpgradeService service = new IndexUpgradeService(Arrays.asList(
                upgradeBarCheck,
                reindexFooCheck
        ));

        IndexMetaData goodIndex = newTestIndexMeta("good", Settings.EMPTY);
        IndexMetaData badIndex = newTestIndexMeta("bad",
                Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.fromString("2.0.0")).build());

        ClusterState clusterState = mockClusterState(goodIndex, badIndex);

        Map<String, UpgradeActionRequired> result = service.upgradeInfo(new String[]{"good", "bad"},
                IndicesOptions.lenientExpandOpen(), clusterState);

        assertThat(result.size(), equalTo(1));
        assertThat(result.get("bad"), equalTo(UpgradeActionRequired.REINDEX));

    }


    private ClusterState mockClusterState(IndexMetaData... indices) {
        MetaData.Builder metaDataBuilder = MetaData.builder();
        for (IndexMetaData indexMetaData : indices) {
            metaDataBuilder.put(indexMetaData, false);
        }
        return ClusterState.builder(ClusterName.DEFAULT).metaData(metaDataBuilder).build();
    }

    public static IndexMetaData newTestIndexMeta(String name, String alias, Settings indexSettings) throws IOException {
        Settings build = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_CREATION_DATE, 1)
                .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                .put(IndexMetaData.SETTING_VERSION_UPGRADED, Version.V_6_0_0)
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
