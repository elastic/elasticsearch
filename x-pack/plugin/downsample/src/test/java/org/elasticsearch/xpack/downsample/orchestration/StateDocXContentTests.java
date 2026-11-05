/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample.orchestration;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.downsample.DownsampleShardProgress;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.downsample.orchestration.OrchestrationTestUtils.newIndexTask;
import static org.elasticsearch.xpack.downsample.orchestration.OrchestrationTestUtils.newShardTask;
import static org.elasticsearch.xpack.downsample.orchestration.OrchestrationTestUtils.roundTrip;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class StateDocXContentTests extends ESTestCase {

    public void testIndexTaskRoundTrip() throws IOException {
        IndexTaskDoc doc = newIndexTask("target");
        assertThat(roundTrip(doc, IndexTaskDoc::fromXContent), equalTo(doc));

        IndexTaskDoc progressed = doc.targetCreated(randomUUID()).withRetry("boom", randomLongBetween(1, 1000));
        assertThat(roundTrip(progressed, IndexTaskDoc::fromXContent), equalTo(progressed));

        IndexTaskDoc failed = progressed.failed("gave up");
        assertThat(roundTrip(failed, IndexTaskDoc::fromXContent), equalTo(failed));
    }

    public void testErrorHistoryIsCapped() {
        IndexTaskDoc doc = newIndexTask("target");
        int failures = IndexTaskDoc.MAX_ERROR_HISTORY + between(1, 5);
        for (int i = 0; i < failures; i++) {
            doc = doc.withRetry("error-" + i, i + 1);
        }
        assertThat(doc.attempt(), equalTo(failures));
        assertThat(doc.errorHistory(), hasSize(IndexTaskDoc.MAX_ERROR_HISTORY));
        assertThat(doc.errorHistory().get(IndexTaskDoc.MAX_ERROR_HISTORY - 1), equalTo("error-" + (failures - 1)));
        assertThat(doc.lastError(), equalTo("error-" + (failures - 1)));
    }

    public void testPhaseChangeResetsRetries() {
        IndexTaskDoc doc = newIndexTask("target").withRetry("boom", 10).targetCreated(randomUUID());
        assertThat(doc.attempt(), equalTo(0));
        assertThat(doc.nextRetryAt(), equalTo(0L));
        assertThat(doc.errorHistory(), contains("boom"));
    }

    public void testShardTaskRoundTrip() throws IOException {
        ShardTaskDoc doc = newShardTask("target", randomUUID());
        assertThat(roundTrip(doc, ShardTaskDoc::fromXContent), equalTo(doc));

        DownsampleShardProgress progress = new DownsampleShardProgress(doc.sourceShardId(), 1, 2);
        if (randomBoolean()) {
            progress.setTotalShardDocCount(between(1, 100));
            progress.addNumReceived(between(0, 100));
        }
        ShardTaskDoc running = doc.assignTo("node-1", false).running(randomUUID(), ShardTaskDoc.progressOf(progress));
        assertThat(roundTrip(running, ShardTaskDoc::fromXContent), equalTo(running));

        ShardTaskDoc failed = running.failedOnNode("boom", randomBoolean(), ShardTaskDoc.progressOf(progress));
        assertThat(roundTrip(failed, ShardTaskDoc::fromXContent), equalTo(failed));
    }

    /**
     * The params write the shard id as {@code [index][shard]}, which drops the index uuid; the doc must put it back.
     */
    public void testShardTaskKeepsSourceIndexUuid() throws IOException {
        ShardTaskDoc doc = newShardTask("target", randomUUID());
        ShardTaskDoc parsed = roundTrip(doc, ShardTaskDoc::fromXContent);
        assertThat(parsed.sourceShardId(), equalTo(doc.sourceShardId()));
        assertThat(parsed.sourceShardId().getIndex().getUUID(), equalTo(doc.sourceShardId().getIndex().getUUID()));
        assertThat(parsed.id(), equalTo(doc.id()));
    }

    public void testProgressUsesTaskStatusFormat() {
        ShardTaskDoc doc = newShardTask("target", randomUUID());
        DownsampleShardProgress progress = new DownsampleShardProgress(doc.sourceShardId(), 1, 2);
        progress.setTotalShardDocCount(10);
        progress.addNumReceived(7);
        progress.addNumIndexed(3);
        Map<String, Object> map = ShardTaskDoc.progressOf(progress);
        assertThat(map.get("total_shard_doc_count"), equalTo(10));
        assertThat(map.get("in_num_docs_received"), equalTo(7));
        assertThat(map.get("out_num_docs_indexed"), equalTo(3));
    }
}
