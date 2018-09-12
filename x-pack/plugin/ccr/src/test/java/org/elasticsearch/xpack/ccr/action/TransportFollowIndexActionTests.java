/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData.State;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MapperTestUtils;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ccr.CcrSettings;
import org.elasticsearch.xpack.ccr.ShardChangesIT;
import org.elasticsearch.xpack.core.ccr.action.FollowIndexAction;

import java.io.IOException;

import static org.elasticsearch.xpack.ccr.action.TransportFollowIndexAction.validate;
import static org.hamcrest.Matchers.equalTo;

public class TransportFollowIndexActionTests extends ESTestCase {

    public void testValidation() throws IOException {
        FollowIndexAction.Request request = ShardChangesIT.createFollowRequest("index1", "index2");
        {
            // should fail, because leader index does not exist
            Exception e = expectThrows(IllegalArgumentException.class, () -> validate(request, null, null, null));
            assertThat(e.getMessage(), equalTo("leader index [index1] does not exist"));
        }
        {
            // should fail, because follow index does not exist
            IndexMetaData leaderIMD = createIMD("index1", 5, Settings.EMPTY);
            Exception e = expectThrows(IllegalArgumentException.class, () -> validate(request, leaderIMD, null, null));
            assertThat(e.getMessage(), equalTo("follow index [index2] does not exist"));
        }
        {
            // should fail because leader index does not have soft deletes enabled
            IndexMetaData leaderIMD = createIMD("index1", 5, Settings.EMPTY);
            IndexMetaData followIMD = createIMD("index2", 5, Settings.EMPTY);
            Exception e = expectThrows(IllegalArgumentException.class, () -> validate(request, leaderIMD, followIMD, null));
            assertThat(e.getMessage(), equalTo("leader index [index1] does not have soft deletes enabled"));
        }
        {
            // should fail because the number of primary shards between leader and follow index are not equal
            IndexMetaData leaderIMD = createIMD("index1", 5, Settings.builder()
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true").build());
            IndexMetaData followIMD = createIMD("index2", 4, Settings.EMPTY);
            Exception e = expectThrows(IllegalArgumentException.class, () -> validate(request, leaderIMD, followIMD, null));
            assertThat(e.getMessage(),
                equalTo("leader index primary shards [5] does not match with the number of shards of the follow index [4]"));
        }
        {
            // should fail, because leader index is closed
            IndexMetaData leaderIMD = createIMD("index1", State.CLOSE, "{}", 5, Settings.builder()
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true").build());
            IndexMetaData followIMD = createIMD("index2", State.OPEN, "{}", 5, Settings.builder()
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true").build());
            Exception e = expectThrows(IllegalArgumentException.class, () -> validate(request, leaderIMD, followIMD, null));
            assertThat(e.getMessage(), equalTo("leader and follow index must be open"));
        }
        {
            // should fail, because leader has a field with the same name mapped as keyword and follower as text
            IndexMetaData leaderIMD = createIMD("index1", State.OPEN, "{\"properties\": {\"field\": {\"type\": \"keyword\"}}}", 5,
                Settings.builder().put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true").build());
            IndexMetaData followIMD = createIMD("index2", State.OPEN, "{\"properties\": {\"field\": {\"type\": \"text\"}}}", 5,
                Settings.builder().put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true).build());
            MapperService mapperService = MapperTestUtils.newMapperService(xContentRegistry(), createTempDir(), Settings.EMPTY, "index2");
            mapperService.updateMapping(null, followIMD);
            Exception e = expectThrows(IllegalArgumentException.class, () -> validate(request, leaderIMD, followIMD, mapperService));
            assertThat(e.getMessage(), equalTo("mapper [field] of different type, current_type [text], merged_type [keyword]"));
        }
        {
            // should fail because of non whitelisted settings not the same between leader and follow index
            String mapping = "{\"properties\": {\"field\": {\"type\": \"text\", \"analyzer\": \"my_analyzer\"}}}";
            IndexMetaData leaderIMD = createIMD("index1", State.OPEN, mapping, 5, Settings.builder()
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true")
                .put("index.analysis.analyzer.my_analyzer.type", "custom")
                .put("index.analysis.analyzer.my_analyzer.tokenizer", "whitespace").build());
            IndexMetaData followIMD = createIMD("index2", State.OPEN, mapping, 5, Settings.builder()
                .put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true)
                .put("index.analysis.analyzer.my_analyzer.type", "custom")
                .put("index.analysis.analyzer.my_analyzer.tokenizer", "standard").build());
            Exception e = expectThrows(IllegalArgumentException.class, () -> validate(request, leaderIMD, followIMD, null));
            assertThat(e.getMessage(), equalTo("the leader and follower index settings must be identical"));
        }
        {
            // should fail because the following index does not have the following_index settings
            IndexMetaData leaderIMD = createIMD("index1", 5,
                Settings.builder().put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true").build());
            Settings followingIndexSettings = randomBoolean() ? Settings.EMPTY :
                Settings.builder().put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), false).build();
            IndexMetaData followIMD = createIMD("index2", 5, followingIndexSettings);
            MapperService mapperService = MapperTestUtils.newMapperService(xContentRegistry(), createTempDir(),
                followingIndexSettings, "index2");
            mapperService.updateMapping(null, followIMD);
            IllegalArgumentException error =
                    expectThrows(IllegalArgumentException.class, () -> validate(request, leaderIMD, followIMD, mapperService));
            assertThat(error.getMessage(), equalTo("the following index [index2] is not ready to follow; " +
                "the setting [index.xpack.ccr.following_index] must be enabled."));
        }
        {
            // should succeed
            IndexMetaData leaderIMD = createIMD("index1", 5, Settings.builder()
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true").build());
            IndexMetaData followIMD = createIMD("index2", 5, Settings.builder()
                .put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true).build());
            MapperService mapperService = MapperTestUtils.newMapperService(xContentRegistry(), createTempDir(), Settings.EMPTY, "index2");
            mapperService.updateMapping(null, followIMD);
            validate(request, leaderIMD, followIMD, mapperService);
        }
        {
            // should succeed, index settings are identical
            String mapping = "{\"properties\": {\"field\": {\"type\": \"text\", \"analyzer\": \"my_analyzer\"}}}";
            IndexMetaData leaderIMD = createIMD("index1", State.OPEN, mapping, 5, Settings.builder()
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true")
                .put("index.analysis.analyzer.my_analyzer.type", "custom")
                .put("index.analysis.analyzer.my_analyzer.tokenizer", "standard").build());
            IndexMetaData followIMD = createIMD("index2", State.OPEN, mapping, 5, Settings.builder()
                .put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true)
                .put("index.analysis.analyzer.my_analyzer.type", "custom")
                .put("index.analysis.analyzer.my_analyzer.tokenizer", "standard").build());
            MapperService mapperService = MapperTestUtils.newMapperService(xContentRegistry(), createTempDir(),
                followIMD.getSettings(), "index2");
            mapperService.updateMapping(null, followIMD);
            validate(request, leaderIMD, followIMD, mapperService);
        }
        {
            // should succeed despite whitelisted settings being different
            String mapping = "{\"properties\": {\"field\": {\"type\": \"text\", \"analyzer\": \"my_analyzer\"}}}";
            IndexMetaData leaderIMD = createIMD("index1", State.OPEN, mapping, 5, Settings.builder()
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true")
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "1s")
                .put("index.analysis.analyzer.my_analyzer.type", "custom")
                .put("index.analysis.analyzer.my_analyzer.tokenizer", "standard").build());
            IndexMetaData followIMD = createIMD("index2", State.OPEN, mapping, 5, Settings.builder()
                .put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true)
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "10s")
                .put("index.analysis.analyzer.my_analyzer.type", "custom")
                .put("index.analysis.analyzer.my_analyzer.tokenizer", "standard").build());
            MapperService mapperService = MapperTestUtils.newMapperService(xContentRegistry(), createTempDir(),
                followIMD.getSettings(), "index2");
            mapperService.updateMapping(null, followIMD);
            validate(request, leaderIMD, followIMD, mapperService);
        }
    }

    private static IndexMetaData createIMD(String index, int numberOfShards, Settings settings) throws IOException {
        return createIMD(index, State.OPEN, "{\"properties\": {}}", numberOfShards, settings);
    }

    private static IndexMetaData createIMD(String index, State state, String mapping, int numberOfShards,
                                           Settings settings) throws IOException {
        return IndexMetaData.builder(index)
            .settings(settings(Version.CURRENT).put(settings))
            .numberOfShards(numberOfShards)
            .state(state)
            .numberOfReplicas(0)
            .setRoutingNumShards(numberOfShards)
            .putMapping("_doc", mapping)
            .build();
    }

}
