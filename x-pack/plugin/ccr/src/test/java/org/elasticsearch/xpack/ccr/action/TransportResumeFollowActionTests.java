/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata.State;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MapperTestUtils;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.ccr.CcrSettings;
import org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.xpack.ccr.action.TransportResumeFollowAction.validate;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class TransportResumeFollowActionTests extends ESTestCase {

    public static ResumeFollowAction.Request resumeFollow(String followerIndex) {
        ResumeFollowAction.Request request = new ResumeFollowAction.Request();
        request.setFollowerIndex(followerIndex);
        request.getParameters().setMaxRetryDelay(TimeValue.timeValueMillis(10));
        request.getParameters().setReadPollTimeout(TimeValue.timeValueMillis(10));
        return request;
    }

    public void testValidation() throws IOException {
        final Map<String, String> customMetadata = new HashMap<>();
        customMetadata.put(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_SHARD_HISTORY_UUIDS, "uuid");
        customMetadata.put(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_UUID_KEY, "_na_");

        ResumeFollowAction.Request request = resumeFollow("index2");
        String[] UUIDs = new String[] { "uuid" };
        {
            IndexMetadata leaderIMD = createIMD("index1", 5, Settings.EMPTY, null);
            IndexMetadata followIMD = createIMD("index2", 5, Settings.EMPTY, null);
            Exception e = expectThrows(IllegalArgumentException.class, () -> validate(request, leaderIMD, followIMD, UUIDs, null));
            assertThat(e.getMessage(), equalTo("follow index [index2] does not have ccr metadata"));
        }
        {
            // should fail because the recorded leader index uuid is not equal to the leader actual index
            IndexMetadata leaderIMD = createIMD("index1", 5, Settings.EMPTY, null);
            IndexMetadata followIMD = createIMD(
                "index2",
                5,
                Settings.EMPTY,
                singletonMap(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_UUID_KEY, "another-value")
            );
            Exception e = expectThrows(IllegalArgumentException.class, () -> validate(request, leaderIMD, followIMD, UUIDs, null));
            assertThat(
                e.getMessage(),
                equalTo(
                    "follow index [index2] should reference [_na_] as leader index but "
                        + "instead reference [another-value] as leader index"
                )
            );
        }
        {
            // should fail because the recorded leader index history uuid is not equal to the leader actual index history uuid:
            IndexMetadata leaderIMD = createIMD("index1", 5, Settings.EMPTY, null);
            Map<String, String> anotherCustomMetadata = new HashMap<>();
            anotherCustomMetadata.put(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_UUID_KEY, "_na_");
            anotherCustomMetadata.put(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_SHARD_HISTORY_UUIDS, "another-uuid");
            IndexMetadata followIMD = createIMD("index2", 5, Settings.EMPTY, anotherCustomMetadata);
            Exception e = expectThrows(IllegalArgumentException.class, () -> validate(request, leaderIMD, followIMD, UUIDs, null));
            assertThat(
                e.getMessage(),
                equalTo(
                    "leader shard [index2][0] should reference [another-uuid] as history uuid but "
                        + "instead reference [uuid] as history uuid"
                )
            );
        }
        {
            // should fail because leader index does not have soft deletes enabled
            IndexMetadata leaderIMD = createIMD(
                "index1",
                5,
                Settings.builder().put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "false").build(),
                null
            );
            IndexMetadata followIMD = createIMD("index2", 5, Settings.EMPTY, customMetadata);
            Exception e = expectThrows(IllegalArgumentException.class, () -> validate(request, leaderIMD, followIMD, UUIDs, null));
            assertThat(e.getMessage(), equalTo("leader index [index1] does not have soft deletes enabled"));
        }
        {
            // should fail because the follower index does not have soft deletes enabled
            IndexMetadata leaderIMD = createIMD("index1", 5, Settings.EMPTY, null);
            IndexMetadata followIMD = createIMD(
                "index2",
                5,
                Settings.builder().put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "false").build(),
                customMetadata
            );
            Exception e = expectThrows(IllegalArgumentException.class, () -> validate(request, leaderIMD, followIMD, UUIDs, null));
            assertThat(e.getMessage(), equalTo("follower index [index2] does not have soft deletes enabled"));
        }
        {
            // should fail because the number of primary shards between leader and follow index are not equal
            IndexMetadata leaderIMD = createIMD("index1", 5, Settings.EMPTY, null);
            IndexMetadata followIMD = createIMD("index2", 4, Settings.EMPTY, customMetadata);
            Exception e = expectThrows(IllegalArgumentException.class, () -> validate(request, leaderIMD, followIMD, UUIDs, null));
            assertThat(
                e.getMessage(),
                equalTo("leader index primary shards [5] does not match with the number of shards of the follow index [4]")
            );
        }
        {
            // should fail, because leader index is closed
            IndexMetadata leaderIMD = createIMD("index1", State.CLOSE, "{}", 5, Settings.EMPTY, null);
            IndexMetadata followIMD = createIMD("index2", State.OPEN, "{}", 5, Settings.EMPTY, customMetadata);
            Exception e = expectThrows(IllegalArgumentException.class, () -> validate(request, leaderIMD, followIMD, UUIDs, null));
            assertThat(e.getMessage(), equalTo("leader and follow index must be open"));
        }
        {
            // should fail, because index.xpack.ccr.following_index setting has not been enabled in leader index
            IndexMetadata leaderIMD = createIMD("index1", 1, Settings.EMPTY, null);
            IndexMetadata followIMD = createIMD("index2", 1, Settings.EMPTY, customMetadata);
            MapperService mapperService = MapperTestUtils.newMapperService(xContentRegistry(), createTempDir(), Settings.EMPTY, "index2");
            mapperService.merge(followIMD, MapperService.MergeReason.MAPPING_RECOVERY);
            Exception e = expectThrows(IllegalArgumentException.class, () -> validate(request, leaderIMD, followIMD, UUIDs, mapperService));
            assertThat(
                e.getMessage(),
                equalTo(
                    "the following index [index2] is not ready to follow; "
                        + "the setting [index.xpack.ccr.following_index] must be enabled."
                )
            );
        }
        {
            // should fail, because leader has a field with the same name mapped as keyword and follower as text
            IndexMetadata leaderIMD = createIMD("index1", State.OPEN, """
                {"properties": {"field": {"type": "keyword"}}}""", 5, Settings.EMPTY, null);
            IndexMetadata followIMD = createIMD(
                "index2",
                State.OPEN,
                """
                    {"properties": {"field": {"type": "text"}}}""",
                5,
                Settings.builder().put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true).build(),
                customMetadata
            );
            MapperService mapperService = MapperTestUtils.newMapperService(xContentRegistry(), createTempDir(), Settings.EMPTY, "index2");
            mapperService.merge(followIMD, MapperService.MergeReason.MAPPING_RECOVERY);
            Exception e = expectThrows(IllegalArgumentException.class, () -> validate(request, leaderIMD, followIMD, UUIDs, mapperService));
            assertThat(e.getMessage(), equalTo("mapper [field] cannot be changed from type [text] to [keyword]"));
        }
        {
            // should fail because of non whitelisted settings not the same between leader and follow index
            String mapping = """
                {"properties": {"field": {"type": "text", "analyzer": "my_analyzer"}}}""";
            IndexMetadata leaderIMD = createIMD(
                "index1",
                State.OPEN,
                mapping,
                5,
                Settings.builder()
                    .put("index.analysis.analyzer.my_analyzer.type", "custom")
                    .put("index.analysis.analyzer.my_analyzer.tokenizer", "whitespace")
                    .build(),
                null
            );
            IndexMetadata followIMD = createIMD(
                "index2",
                State.OPEN,
                mapping,
                5,
                Settings.builder()
                    .put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true)
                    .put("index.analysis.analyzer.my_analyzer.type", "custom")
                    .put("index.analysis.analyzer.my_analyzer.tokenizer", "standard")
                    .build(),
                customMetadata
            );
            Exception e = expectThrows(IllegalArgumentException.class, () -> validate(request, leaderIMD, followIMD, UUIDs, null));
            assertThat(e.getMessage(), equalTo("""
                the leader index settings [{"index.analysis.analyzer.my_analyzer.tokenizer":"whitespace",\
                "index.analysis.analyzer.my_analyzer.type":"custom","index.number_of_shards":"5"}] and follower index settings \
                [{"index.analysis.analyzer.my_analyzer.tokenizer":"standard","index.analysis.analyzer.my_analyzer.type":"custom",\
                "index.number_of_shards":"5"}] must be identical"""));
        }
        {
            // should fail because the following index does not have the following_index settings
            IndexMetadata leaderIMD = createIMD("index1", 5, Settings.EMPTY, null);
            Settings followingIndexSettings = Settings.builder().put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), false).build();
            IndexMetadata followIMD = createIMD("index2", 5, followingIndexSettings, customMetadata);
            MapperService mapperService = MapperTestUtils.newMapperService(
                xContentRegistry(),
                createTempDir(),
                followingIndexSettings,
                "index2"
            );
            mapperService.merge(followIMD, MapperService.MergeReason.MAPPING_RECOVERY);
            IllegalArgumentException error = expectThrows(
                IllegalArgumentException.class,
                () -> validate(request, leaderIMD, followIMD, UUIDs, mapperService)
            );
            assertThat(
                error.getMessage(),
                equalTo(
                    "the following index [index2] is not ready to follow; "
                        + "the setting [index.xpack.ccr.following_index] must be enabled."
                )
            );
        }
        {
            // should succeed
            IndexMetadata leaderIMD = createIMD("index1", 5, Settings.EMPTY, null);
            IndexMetadata followIMD = createIMD(
                "index2",
                5,
                Settings.builder().put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true).build(),
                customMetadata
            );
            MapperService mapperService = MapperTestUtils.newMapperService(xContentRegistry(), createTempDir(), Settings.EMPTY, "index2");
            mapperService.merge(followIMD, MapperService.MergeReason.MAPPING_RECOVERY);
            validate(request, leaderIMD, followIMD, UUIDs, mapperService);
        }
        {
            // should succeed, index settings are identical
            String mapping = """
                {"properties": {"field": {"type": "text", "analyzer": "my_analyzer"}}}""";
            IndexMetadata leaderIMD = createIMD(
                "index1",
                State.OPEN,
                mapping,
                5,
                Settings.builder()
                    .put("index.analysis.analyzer.my_analyzer.type", "custom")
                    .put("index.analysis.analyzer.my_analyzer.tokenizer", "standard")
                    .build(),
                null
            );
            IndexMetadata followIMD = createIMD(
                "index2",
                State.OPEN,
                mapping,
                5,
                Settings.builder()
                    .put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true)
                    .put("index.analysis.analyzer.my_analyzer.type", "custom")
                    .put("index.analysis.analyzer.my_analyzer.tokenizer", "standard")
                    .build(),
                customMetadata
            );
            MapperService mapperService = MapperTestUtils.newMapperService(
                xContentRegistry(),
                createTempDir(),
                followIMD.getSettings(),
                "index2"
            );
            mapperService.merge(followIMD, MapperService.MergeReason.MAPPING_RECOVERY);
            validate(request, leaderIMD, followIMD, UUIDs, mapperService);
        }
        {
            // should succeed despite whitelisted settings being different
            String mapping = """
                {"properties": {"field": {"type": "text", "analyzer": "my_analyzer"}}}""";
            IndexMetadata leaderIMD = createIMD(
                "index1",
                State.OPEN,
                mapping,
                5,
                Settings.builder()
                    .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "1s")
                    .put("index.analysis.analyzer.my_analyzer.type", "custom")
                    .put("index.analysis.analyzer.my_analyzer.tokenizer", "standard")
                    .build(),
                null
            );
            IndexMetadata followIMD = createIMD(
                "index2",
                State.OPEN,
                mapping,
                5,
                Settings.builder()
                    .put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true)
                    .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "10s")
                    .put("index.analysis.analyzer.my_analyzer.type", "custom")
                    .put("index.analysis.analyzer.my_analyzer.tokenizer", "standard")
                    .build(),
                customMetadata
            );
            MapperService mapperService = MapperTestUtils.newMapperService(
                xContentRegistry(),
                createTempDir(),
                followIMD.getSettings(),
                "index2"
            );
            mapperService.merge(followIMD, MapperService.MergeReason.MAPPING_RECOVERY);
            validate(request, leaderIMD, followIMD, UUIDs, mapperService);
        }
    }

    public void testDynamicIndexSettingsAreClassified() {
        // We should be conscious which dynamic settings are replicated from leader to follower index.
        // This is the list of settings that should be replicated:
        Set<Setting<?>> replicatedSettings = new HashSet<>();

        // These fields need to be replicated otherwise documents that can be indexed in the leader index cannot
        // be indexed in the follower index:
        replicatedSettings.add(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING);
        replicatedSettings.add(MapperService.INDEX_MAPPING_NESTED_DOCS_LIMIT_SETTING);
        replicatedSettings.add(MapperService.INDEX_MAPPING_NESTED_FIELDS_LIMIT_SETTING);
        replicatedSettings.add(MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING);
        replicatedSettings.add(MapperService.INDEX_MAPPING_FIELD_NAME_LENGTH_LIMIT_SETTING);
        replicatedSettings.add(MapperService.INDEX_MAPPING_DIMENSION_FIELDS_LIMIT_SETTING);
        replicatedSettings.add(IndexSettings.LIFECYCLE_ORIGINATION_DATE_SETTING);
        replicatedSettings.add(IndexSettings.LIFECYCLE_PARSE_ORIGINATION_DATE_SETTING);
        replicatedSettings.add(IndexSettings.MAX_NGRAM_DIFF_SETTING);
        replicatedSettings.add(IndexSettings.MAX_SHINGLE_DIFF_SETTING);
        replicatedSettings.add(IndexSettings.TIME_SERIES_END_TIME);
        replicatedSettings.add(IndexSettings.PREFER_ILM_SETTING);

        for (Setting<?> setting : IndexScopedSettings.BUILT_IN_INDEX_SETTINGS) {
            // removed settings have no effect, they are only there for BWC
            if (setting.isDynamic() && setting.isDeprecatedAndRemoved() == false) {
                boolean notReplicated = TransportResumeFollowAction.NON_REPLICATED_SETTINGS.contains(setting);
                boolean replicated = replicatedSettings.contains(setting);
                assertThat(
                    "setting [" + setting.getKey() + "] is not classified as replicated or not replicated",
                    notReplicated ^ replicated,
                    is(true)
                );
            }
        }
    }

    public void testFilter() {
        Settings.Builder settings = Settings.builder();
        settings.put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), "");
        settings.put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "");
        settings.put(IndexMetadata.SETTING_VERSION_CREATED, "");
        settings.put(IndexMetadata.SETTING_INDEX_UUID, "");
        settings.put(IndexMetadata.SETTING_INDEX_PROVIDED_NAME, "");
        settings.put(IndexMetadata.SETTING_CREATION_DATE, "");
        settings.put(IndexMetadata.SETTING_VERSION_UPGRADED, "");
        settings.put(IndexMetadata.SETTING_VERSION_UPGRADED_STRING, "");

        Settings result = TransportResumeFollowAction.filter(settings.build());
        assertThat(result.size(), equalTo(0));
    }

    private static IndexMetadata createIMD(String index, int numberOfShards, Settings settings, Map<String, String> custom)
        throws IOException {
        return createIMD(index, State.OPEN, "{\"properties\": {}}", numberOfShards, settings, custom);
    }

    private static IndexMetadata createIMD(
        String index,
        State state,
        String mapping,
        int numberOfShards,
        Settings settings,
        Map<String, String> custom
    ) throws IOException {
        IndexMetadata.Builder builder = IndexMetadata.builder(index)
            .settings(settings(Version.CURRENT).put(settings))
            .numberOfShards(numberOfShards)
            .state(state)
            .numberOfReplicas(0)
            .setRoutingNumShards(numberOfShards)
            .putMapping(mapping);

        if (custom != null) {
            builder.putCustom(Ccr.CCR_CUSTOM_METADATA_KEY, custom);
        }

        return builder.build();
    }

}
