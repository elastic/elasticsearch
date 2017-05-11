/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.upgrade;


import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.xpack.upgrade.IndexUpgradeCheckTests.newTestIndexMeta;
import static org.hamcrest.core.IsEqual.equalTo;

public class IndexUpgradeServiceTests extends ESTestCase {

    private IndexUpgradeCheck upgradeBarCheck = new IndexUpgradeCheck() {
        @Override
        public String getName() {
            return "upgrade_bar";
        }

        @Override
        public UpgradeActionRequired actionRequired(IndexMetaData indexMetaData, Map<String, String> params, ClusterState state) {
            if ("bar".equals(indexMetaData.getSettings().get("test.setting"))) {
                return UpgradeActionRequired.UPGRADE;
            }
            return UpgradeActionRequired.NOT_APPLICABLE;
        }
    };

    private IndexUpgradeCheck reindexFooCheck = new IndexUpgradeCheck() {
        @Override
        public String getName() {
            return "reindex_foo";
        }

        @Override
        public UpgradeActionRequired actionRequired(IndexMetaData indexMetaData, Map<String, String> params, ClusterState state) {
            if ("foo".equals(indexMetaData.getSettings().get("test.setting"))) {
                return UpgradeActionRequired.REINDEX;
            }
            return UpgradeActionRequired.NOT_APPLICABLE;
        }
    };

    private IndexUpgradeCheck everythingIsFineCheck = new IndexUpgradeCheck() {
        @Override
        public String getName() {
            return "everything_is_fine";
        }

        @Override
        public UpgradeActionRequired actionRequired(IndexMetaData indexMetaData, Map<String, String> params, ClusterState state) {
            return UpgradeActionRequired.UP_TO_DATE;
        }
    };

    private IndexUpgradeCheck unreachableCheck = new IndexUpgradeCheck() {

        @Override
        public String getName() {
            return "unreachable";
        }

        @Override
        public UpgradeActionRequired actionRequired(IndexMetaData indexMetaData, Map<String, String> params, ClusterState state) {
            fail("Unreachable check is called");
            return null;
        }
    };

    public void testIndexUpgradeServiceMultipleCheck() {
        IndexUpgradeService service;
        if (randomBoolean()) {
            service = new IndexUpgradeService(Settings.EMPTY, Arrays.asList(
                    upgradeBarCheck,
                    reindexFooCheck,
                    everythingIsFineCheck,
                    unreachableCheck // This one should never be called
            ));
        } else {
            service = new IndexUpgradeService(Settings.EMPTY, Arrays.asList(
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
                IndicesOptions.lenientExpandOpen(), Collections.emptyMap(), clusterState);

        assertThat(result.size(), equalTo(2));
        assertThat(result.get("bar"), equalTo(UpgradeActionRequired.UPGRADE));
        assertThat(result.get("foo"), equalTo(UpgradeActionRequired.REINDEX));

        result = service.upgradeInfo(new String[]{"b*"}, IndicesOptions.lenientExpandOpen(), Collections.emptyMap(), clusterState);

        assertThat(result.size(), equalTo(1));
        assertThat(result.get("bar"), equalTo(UpgradeActionRequired.UPGRADE));
    }


    public void testNoMatchingChecks() {
        IndexUpgradeService service = new IndexUpgradeService(Settings.EMPTY, Arrays.asList(
                upgradeBarCheck,
                reindexFooCheck
        ));

        IndexMetaData fooIndex = newTestIndexMeta("bar", Settings.builder().put("test.setting", "bar").build());
        IndexMetaData barIndex = newTestIndexMeta("foo", Settings.builder().put("test.setting", "foo").build());
        IndexMetaData bazIndex = newTestIndexMeta("baz", Settings.EMPTY);

        ClusterState clusterState = mockClusterState(fooIndex, barIndex, bazIndex);

        Map<String, UpgradeActionRequired> result = service.upgradeInfo(new String[]{"bar", "foo", "baz"},
                IndicesOptions.lenientExpandOpen(), Collections.emptyMap(), clusterState);

        assertThat(result.size(), equalTo(2));
        assertThat(result.get("bar"), equalTo(UpgradeActionRequired.UPGRADE));
        assertThat(result.get("foo"), equalTo(UpgradeActionRequired.REINDEX));
    }

    public void testEarlierChecksWin() {

        IndexUpgradeService service = new IndexUpgradeService(Settings.EMPTY, Arrays.asList(
                everythingIsFineCheck,
                upgradeBarCheck,
                reindexFooCheck
        ));

        IndexMetaData fooIndex = newTestIndexMeta("bar", Settings.builder().put("test.setting", "bar").build());
        IndexMetaData barIndex = newTestIndexMeta("foo", Settings.builder().put("test.setting", "foo").build());
        IndexMetaData bazIndex = newTestIndexMeta("baz", Settings.EMPTY);

        ClusterState clusterState = mockClusterState(fooIndex, barIndex, bazIndex);

        Map<String, UpgradeActionRequired> result = service.upgradeInfo(new String[]{"bar", "foo", "baz"},
                IndicesOptions.lenientExpandOpen(), Collections.emptyMap(), clusterState);

        assertThat(result.size(), equalTo(0)); // everything as the first checker should indicate that everything is fine

    }

    private ClusterState mockClusterState(IndexMetaData... indices) {
        MetaData.Builder metaDataBuilder = MetaData.builder();
        for (IndexMetaData indexMetaData : indices) {
            metaDataBuilder.put(indexMetaData, false);
        }
        return ClusterState.builder(ClusterName.DEFAULT).metaData(metaDataBuilder).build();
    }
}
