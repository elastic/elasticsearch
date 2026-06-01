/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.transform.action.StartTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.DestConfig;
import org.elasticsearch.xpack.core.transform.transforms.QueryConfig;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TimeSyncConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.latest.LatestConfig;
import org.elasticsearch.xpack.transform.TransformSingleNodeTestCase;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

public class TransformInitialDelayIT extends TransformSingleNodeTestCase {

    private static final String SOURCE_INDEX = "initial-delay-source";
    // A long steady-state delay: without an initial_delay override the first checkpoint upper bound would be (now - 60s), so a
    // document timestamped a few seconds ago would NOT be processed for ~60s.
    private static final TimeValue SYNC_DELAY = TimeValue.timeValueSeconds(60);
    private static final TimeValue FREQUENCY = TimeValue.timeValueHours(1);

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(super.nodeSettings()).put(XPackSettings.SECURITY_ENABLED.getKey(), false).build();
    }

    /**
     * With initial_delay=0s the first checkpoint processes data up to "now", so an already-present (backfilled) document is
     * summarized into the destination index immediately, despite a conservative 60s steady-state delay. Without the override the
     * document would only appear after it ages past the 60s watermark.
     */
    public void testInitialDelayProcessesBackfillImmediately() throws Exception {
        String transformId = "test-initial-delay";
        String destIndex = transformId + "-dest";

        createSourceIndexWithMapping();
        // Timestamp the document a few seconds in the past: it is below (now), so initial_delay=0s includes it on checkpoint #1,
        // but it is above (now - 60s), so it would have been excluded by the steady-state delay alone.
        indexDoc(Instant.now().minusSeconds(5).toEpochMilli());
        // Ensure the backfilled document is searchable before the first checkpoint runs. This mirrors the documented guidance that
        // initial_delay should be >= the source refresh interval so checkpoint #1 does not race document visibility.
        indicesAdmin().refresh(new RefreshRequest(SOURCE_INDEX)).actionGet();

        TransformConfig config = TransformConfig.builder()
            .setId(transformId)
            .setSource(new SourceConfig(new String[] { SOURCE_INDEX }, QueryConfig.matchAll(), Map.of(), null))
            .setDest(new DestConfig(destIndex, null, null))
            .setFrequency(FREQUENCY)
            .setSyncConfig(new TimeSyncConfig("time", SYNC_DELAY, TimeValue.ZERO))
            .setLatestConfig(new LatestConfig(List.of("key"), "time"))
            .build();
        createTransform(config);

        client().execute(StartTransformAction.INSTANCE, new StartTransformAction.Request(transformId, null, TimeValue.THIRTY_SECONDS))
            .actionGet(TimeValue.THIRTY_SECONDS);

        // The first checkpoint runs right after start. Because initial_delay=0s, it covers the just-indexed document and writes it to
        // the destination well within the 60s steady-state delay window.
        assertBusy(() -> assertThat(destDocCount(destIndex), equalTo(1L)), 30, TimeUnit.SECONDS);
    }

    private long destDocCount(String index) {
        indicesAdmin().refresh(new RefreshRequest(index).indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)).actionGet();
        SearchRequest searchRequest = new SearchRequest(index).indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
            .source(new SearchSourceBuilder().size(0).trackTotalHits(true));
        var searchResponse = client().search(searchRequest).actionGet(TimeValue.THIRTY_SECONDS);
        try {
            return searchResponse.getHits().getTotalHits().value();
        } finally {
            searchResponse.decRef();
        }
    }

    private void createSourceIndexWithMapping() throws Exception {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.startObject("properties");
            builder.startObject("time").field("type", "date").endObject();
            builder.startObject("key").field("type", "keyword").endObject();
            builder.endObject();
            builder.endObject();
            indicesAdmin().create(new CreateIndexRequest(SOURCE_INDEX).mapping(builder)).actionGet();
        }
    }

    private void indexDoc(long timeMillis) {
        client().bulk(
            new BulkRequest().add(new IndexRequest(SOURCE_INDEX).source(Map.of("time", timeMillis, "key", randomAlphaOfLength(5))))
        ).actionGet(TimeValue.THIRTY_SECONDS);
    }
}
