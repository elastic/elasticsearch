/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.CcrSingleNodeTestCase;
import org.elasticsearch.xpack.core.ccr.action.CcrStatsAction;
import org.elasticsearch.xpack.core.ccr.action.FollowStatsAction;
import org.elasticsearch.xpack.core.ccr.action.PauseFollowAction;
import org.elasticsearch.xpack.core.ccr.action.PutAutoFollowPatternAction;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;
import org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class LocalIndexFollowingIT extends CcrSingleNodeTestCase {

    public void testFollowIndex() throws Exception {
        final String leaderIndexSettings = getIndexSettings(2, 0, Collections.emptyMap());
        assertAcked(client().admin().indices().prepareCreate("leader").setSource(leaderIndexSettings, XContentType.JSON));
        ensureGreen("leader");

        final long firstBatchNumDocs = randomIntBetween(2, 64);
        for (int i = 0; i < firstBatchNumDocs; i++) {
            client().prepareIndex("leader").setSource("{}", XContentType.JSON).get();
        }

        final PutFollowAction.Request followRequest = getPutFollowRequest("leader", "follower");
        client().execute(PutFollowAction.INSTANCE, followRequest).get();

        assertBusy(() -> {
            assertThat(client().prepareSearch("follower").get().getHits().getTotalHits().value, equalTo(firstBatchNumDocs));
        });

        final long secondBatchNumDocs = randomIntBetween(2, 64);
        for (int i = 0; i < secondBatchNumDocs; i++) {
            client().prepareIndex("leader").setSource("{}", XContentType.JSON).get();
        }

        assertBusy(() -> {
            assertThat(client().prepareSearch("follower").get()
                .getHits().getTotalHits().value, equalTo(firstBatchNumDocs + secondBatchNumDocs));
        });

        PauseFollowAction.Request pauseRequest = new PauseFollowAction.Request("follower");
        client().execute(PauseFollowAction.INSTANCE, pauseRequest);

        final long thirdBatchNumDocs = randomIntBetween(2, 64);
        for (int i = 0; i < thirdBatchNumDocs; i++) {
            client().prepareIndex("leader").setSource("{}", XContentType.JSON).get();
        }

        client().execute(ResumeFollowAction.INSTANCE, getResumeFollowRequest("follower")).get();
        assertBusy(() -> {
            assertThat(client().prepareSearch("follower").get().getHits().getTotalHits().value,
                equalTo(firstBatchNumDocs + secondBatchNumDocs + thirdBatchNumDocs));
        });
        ensureEmptyWriteBuffers();
    }

    public void testRemoveRemoteConnection() throws Exception {
        PutAutoFollowPatternAction.Request request = new PutAutoFollowPatternAction.Request();
        request.setName("my_pattern");
        request.setRemoteCluster("local");
        request.setLeaderIndexPatterns(Collections.singletonList("logs-*"));
        request.setFollowIndexNamePattern("copy-{{leader_index}}");
        request.getParameters().setReadPollTimeout(TimeValue.timeValueMillis(10));
        assertTrue(client().execute(PutAutoFollowPatternAction.INSTANCE, request).actionGet().isAcknowledged());
        long previousNumberOfSuccessfulFollowedIndices = getAutoFollowStats().getNumberOfSuccessfulFollowIndices();

        Settings leaderIndexSettings = Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .build();
        createIndex("logs-20200101", leaderIndexSettings);
        client().prepareIndex("logs-20200101").setSource("{}", XContentType.JSON).get();
        assertBusy(() -> {
            CcrStatsAction.Response response = client().execute(CcrStatsAction.INSTANCE, new CcrStatsAction.Request()).actionGet();
            assertThat(response.getAutoFollowStats().getNumberOfSuccessfulFollowIndices(),
                equalTo(previousNumberOfSuccessfulFollowedIndices + 1));
            assertThat(response.getFollowStats().getStatsResponses().size(), equalTo(1));
            assertThat(response.getFollowStats().getStatsResponses().get(0).status().followerGlobalCheckpoint(), equalTo(0L));
        });

        // Both auto follow patterns and index following should be resilient to remote connection being missing:
        removeLocalRemote();
        // This triggers a cluster state update, which should let auto follow coordinator retry auto following:
        setupLocalRemote();

        // This new index should be picked up by auto follow coordinator
        createIndex("logs-20200102", leaderIndexSettings);
        // This new document should be replicated to follower index:
        client().prepareIndex("logs-20200101").setSource("{}", XContentType.JSON).get();
        assertBusy(() -> {
            CcrStatsAction.Response response = client().execute(CcrStatsAction.INSTANCE, new CcrStatsAction.Request()).actionGet();
            assertThat(response.getAutoFollowStats().getNumberOfSuccessfulFollowIndices(),
                equalTo(previousNumberOfSuccessfulFollowedIndices + 2));

            FollowStatsAction.StatsRequest statsRequest = new FollowStatsAction.StatsRequest();
            statsRequest.setIndices(new String[]{"copy-logs-20200101"});
            FollowStatsAction.StatsResponses responses = client().execute(FollowStatsAction.INSTANCE, statsRequest).actionGet();
            assertThat(responses.getStatsResponses().size(), equalTo(1));
            assertThat(responses.getStatsResponses().get(0).status().getFatalException(), nullValue());
            assertThat(responses.getStatsResponses().get(0).status().followerGlobalCheckpoint(), equalTo(1L));
        });
    }

    public static String getIndexSettings(final int numberOfShards,
                                          final int numberOfReplicas,
                                          final Map<String, String> additionalIndexSettings) throws IOException {
        final String settings;
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            {
                builder.startObject("settings");
                {
                    builder.field("index.number_of_shards", numberOfShards);
                    builder.field("index.number_of_replicas", numberOfReplicas);
                    for (final Map.Entry<String, String> additionalSetting : additionalIndexSettings.entrySet()) {
                        builder.field(additionalSetting.getKey(), additionalSetting.getValue());
                    }
                }
                builder.endObject();
            }
            builder.endObject();
            settings = BytesReference.bytes(builder).utf8ToString();
        }
        return settings;
    }

}
