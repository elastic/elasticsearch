/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData.State;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MapperTestUtils;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ccr.ShardChangesIT;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class FollowIndexActionTests extends ESTestCase {

    public void testValidation() throws IOException {
        FollowIndexAction.Request request = ShardChangesIT.createFollowRequest("index1", "index2");
        {
            // should fail, because leader index does not exist
            Exception e = expectThrows(IllegalArgumentException.class, () -> FollowIndexAction.validate(request, null, null, null));
            assertThat(e.getMessage(), equalTo("leader index [index1] does not exist"));
        }
        {
            // should fail, because follow index does not exist
            IndexMetaData leaderIMD = createIMD("index1", 5);
            Exception e = expectThrows(IllegalArgumentException.class, () -> FollowIndexAction.validate(request, leaderIMD, null, null));
            assertThat(e.getMessage(), equalTo("follow index [index2] does not exist"));
        }
        {
            // should fail because leader index does not have soft deletes enabled
            IndexMetaData leaderIMD = createIMD("index1", 5);
            IndexMetaData followIMD = createIMD("index2", 5);
            Exception e = expectThrows(IllegalArgumentException.class,
                () -> FollowIndexAction.validate(request, leaderIMD, followIMD, null));
            assertThat(e.getMessage(), equalTo("leader index [index1] does not have soft deletes enabled"));
        }
        {
            // should fail because the number of primary shards between leader and follow index are not equal
            IndexMetaData leaderIMD = createIMD("index1", 5, new Tuple<>(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"));
            IndexMetaData followIMD = createIMD("index2", 4);
            Exception e = expectThrows(IllegalArgumentException.class,
                () -> FollowIndexAction.validate(request, leaderIMD, followIMD, null));
            assertThat(e.getMessage(),
                equalTo("leader index primary shards [5] does not match with the number of shards of the follow index [4]"));
        }
        {
            // should fail, because leader index is closed
            IndexMetaData leaderIMD = createIMD("index1", State.CLOSE, "{}", 5,
                new Tuple<>(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"));
            IndexMetaData followIMD = createIMD("index2", State.OPEN, "{}", 5,
                new Tuple<>(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"));
            Exception e = expectThrows(IllegalArgumentException.class,
                () -> FollowIndexAction.validate(request, leaderIMD, followIMD, null));
            assertThat(e.getMessage(), equalTo("leader and follow index must be open"));
        }
        {
            // should fail, because leader has a field with the same name mapped as keyword and follower as text
            IndexMetaData leaderIMD = createIMD("index1", State.OPEN, "{\"properties\": {\"field\": {\"type\": \"keyword\"}}}", 5,
                new Tuple<>(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"));
            IndexMetaData followIMD = createIMD("index2", State.OPEN, "{\"properties\": {\"field\": {\"type\": \"text\"}}}", 5);
            MapperService mapperService = MapperTestUtils.newMapperService(xContentRegistry(), createTempDir(), Settings.EMPTY, "index2");
            mapperService.updateMapping(followIMD);
            Exception e = expectThrows(IllegalArgumentException.class,
                () -> FollowIndexAction.validate(request, leaderIMD, followIMD, mapperService));
            assertThat(e.getMessage(), equalTo("mapper [field] of different type, current_type [text], merged_type [keyword]"));
        }
        {
            // should fail because of non whitelisted settings not the same between leader and follow index
            String mapping = "{\"properties\": {\"field\": {\"type\": \"text\", \"analyzer\": \"my_analyzer\"}}}";
            IndexMetaData leaderIMD = createIMD("index1", State.OPEN, mapping, 5,
                new Tuple<>(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"),
                new Tuple<>("index.analysis.analyzer.my_analyzer.type", "custom"),
                new Tuple<>("index.analysis.analyzer.my_analyzer.tokenizer", "whitespace"));
            IndexMetaData followIMD = createIMD("index2", State.OPEN, mapping, 5,
                new Tuple<>("index.analysis.analyzer.my_analyzer.type", "custom"),
                new Tuple<>("index.analysis.analyzer.my_analyzer.tokenizer", "standard"));
            Exception e = expectThrows(IllegalArgumentException.class,
                () -> FollowIndexAction.validate(request, leaderIMD, followIMD, null));
            assertThat(e.getMessage(), equalTo("the leader and follower index settings must be identical"));
        }
        {
            // should succeed
            IndexMetaData leaderIMD = createIMD("index1", 5, new Tuple<>(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"));
            IndexMetaData followIMD = createIMD("index2", 5);
            MapperService mapperService = MapperTestUtils.newMapperService(xContentRegistry(), createTempDir(), Settings.EMPTY, "index2");
            mapperService.updateMapping(followIMD);
            FollowIndexAction.validate(request, leaderIMD, followIMD, mapperService);
        }
        {
            // should succeed, index settings are identical
            String mapping = "{\"properties\": {\"field\": {\"type\": \"text\", \"analyzer\": \"my_analyzer\"}}}";
            IndexMetaData leaderIMD = createIMD("index1", State.OPEN, mapping, 5,
                new Tuple<>(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"),
                new Tuple<>("index.analysis.analyzer.my_analyzer.type", "custom"),
                new Tuple<>("index.analysis.analyzer.my_analyzer.tokenizer", "standard"));
            IndexMetaData followIMD = createIMD("index2", State.OPEN, mapping, 5,
                new Tuple<>("index.analysis.analyzer.my_analyzer.type", "custom"),
                new Tuple<>("index.analysis.analyzer.my_analyzer.tokenizer", "standard"));
            MapperService mapperService = MapperTestUtils.newMapperService(xContentRegistry(), createTempDir(),
                followIMD.getSettings(), "index2");
            mapperService.updateMapping(followIMD);
            FollowIndexAction.validate(request, leaderIMD, followIMD, mapperService);
        }
        {
            // should succeed despite whitelisted settings being different
            String mapping = "{\"properties\": {\"field\": {\"type\": \"text\", \"analyzer\": \"my_analyzer\"}}}";
            IndexMetaData leaderIMD = createIMD("index1", State.OPEN, mapping, 5,
                new Tuple<>(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"),
                new Tuple<>(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "1s"),
                new Tuple<>("index.analysis.analyzer.my_analyzer.type", "custom"),
                new Tuple<>("index.analysis.analyzer.my_analyzer.tokenizer", "standard"));
            IndexMetaData followIMD = createIMD("index2", State.OPEN, mapping, 5,
                new Tuple<>(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "10s"),
                new Tuple<>("index.analysis.analyzer.my_analyzer.type", "custom"),
                new Tuple<>("index.analysis.analyzer.my_analyzer.tokenizer", "standard"));
            MapperService mapperService = MapperTestUtils.newMapperService(xContentRegistry(), createTempDir(),
                followIMD.getSettings(), "index2");
            mapperService.updateMapping(followIMD);
            FollowIndexAction.validate(request, leaderIMD, followIMD, mapperService);
        }
    }

    private static IndexMetaData createIMD(String index, int numShards, Tuple<?, ?>... settings) throws IOException {
        return createIMD(index, State.OPEN, "{\"properties\": {}}", numShards, settings);
    }

    private static IndexMetaData createIMD(String index, State state, String mapping, int numShards,
                                           Tuple<?, ?>... settings) throws IOException {
        Settings.Builder settingsBuilder = settings(Version.CURRENT);
        for (Tuple<?, ?> setting : settings) {
            settingsBuilder.put((String) setting.v1(), (String) setting.v2());
        }
        return IndexMetaData.builder(index).settings(settingsBuilder)
            .numberOfShards(numShards)
            .state(state)
            .numberOfReplicas(0)
            .setRoutingNumShards(numShards)
            .putMapping("_doc", mapping)
            .build();
    }

}
