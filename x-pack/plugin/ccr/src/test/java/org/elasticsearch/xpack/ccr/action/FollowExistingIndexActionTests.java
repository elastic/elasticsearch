/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class FollowExistingIndexActionTests extends ESTestCase {

    public void testValidation() {
        FollowExistingIndexAction.Request request = new FollowExistingIndexAction.Request();
        request.setLeaderIndex("index1");
        request.setFollowIndex("index2");

        {
            Exception e = expectThrows(IllegalArgumentException.class, () -> FollowExistingIndexAction.validate(null, null, request));
            assertThat(e.getMessage(), equalTo("leader index [index1] does not exist"));
        }
        {
            IndexMetaData leaderIMD = createIMD("index1", 5);
            Exception e = expectThrows(IllegalArgumentException.class, () -> FollowExistingIndexAction.validate(leaderIMD, null, request));
            assertThat(e.getMessage(), equalTo("follow index [index2] does not exist"));
        }
        {
            IndexMetaData leaderIMD = createIMD("index1", 5);
            IndexMetaData followIMD = createIMD("index2", 5);
            Exception e = expectThrows(IllegalArgumentException.class,
                () -> FollowExistingIndexAction.validate(leaderIMD, followIMD, request));
            assertThat(e.getMessage(), equalTo("leader index [index1] does not have soft deletes enabled"));
        }
        {
            IndexMetaData leaderIMD = createIMD("index1", 5, new Tuple<>(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"));
            IndexMetaData followIMD = createIMD("index2", 4);
            Exception e = expectThrows(IllegalArgumentException.class,
                () -> FollowExistingIndexAction.validate(leaderIMD, followIMD, request));
            assertThat(e.getMessage(),
                equalTo("leader index primary shards [5] does not match with the number of shards of the follow index [4]"));
        }
        {
            IndexMetaData leaderIMD = createIMD("index1", 5, new Tuple<>(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"));
            IndexMetaData followIMD = createIMD("index2", 5);
            FollowExistingIndexAction.validate(leaderIMD, followIMD, request);
        }
    }

    private static IndexMetaData createIMD(String index, int numShards, Tuple<?, ?>... settings) {
        Settings.Builder settingsBuilder = settings(Version.CURRENT);
        for (Tuple<?, ?> setting : settings) {
            settingsBuilder.put((String) setting.v1(), (String) setting.v2());
        }
        return IndexMetaData.builder(index).settings(settingsBuilder)
            .numberOfShards(numShards)
            .numberOfReplicas(0)
            .setRoutingNumShards(numShards).build();
    }

}
