/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.transform.action.ScheduleNowTransformAction;
import org.elasticsearch.xpack.core.transform.action.StartTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.DestConfig;
import org.elasticsearch.xpack.core.transform.transforms.QueryConfig;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TimeSyncConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.latest.LatestConfig;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.TransformSingleNodeTestCase;
import org.elasticsearch.xpack.transform.transforms.scheduling.TransformScheduler;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

public class TransformScheduleNowIT extends TransformSingleNodeTestCase {

    private static final String SOURCE_INDEX = "schedule-now-source";
    private static final TimeValue SYNC_DELAY = TimeValue.timeValueSeconds(30);
    private static final TimeValue FREQUENCY = TimeValue.timeValueHours(1);

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(super.nodeSettings()).put(XPackSettings.SECURITY_ENABLED.getKey(), false).build();
    }

    public void testScheduleNowWithDefer() throws Exception {
        String transformId = "test-schedule-now-defer";
        TransformScheduler scheduler = createStartAndRegisterTransform(transformId);

        long beforeScheduleNow = System.currentTimeMillis();
        scheduleNowTransform(transformId, true);
        long afterScheduleNow = System.currentTimeMillis();

        Long nextScheduledTime = scheduler.getNextScheduledTimeMillis(transformId);
        assertThat(nextScheduledTime, notNullValue());
        assertThat(nextScheduledTime, greaterThanOrEqualTo(beforeScheduleNow + SYNC_DELAY.millis()));
        assertThat(nextScheduledTime, lessThanOrEqualTo(afterScheduleNow + SYNC_DELAY.millis()));
    }

    public void testScheduleNowWithoutDefer() throws Exception {
        String transformId = "test-schedule-now-no-defer";
        TransformScheduler scheduler = createStartAndRegisterTransform(transformId);

        long beforeScheduleNow = System.currentTimeMillis();
        scheduleNowTransform(transformId, false);
        long afterScheduleNow = System.currentTimeMillis();

        // Without defer, scheduleNow triggers immediately so lastTriggeredTime ~ now, nextScheduledTime ~ now + frequency.
        // Wrap in assertBusy because if the scheduler background thread happens to be inside processScheduledTasks()
        // at the moment scheduleNow is called, the task won't be processed until the next scheduler cycle.
        assertBusy(() -> {
            Long nextScheduledTime = scheduler.getNextScheduledTimeMillis(transformId);
            assertThat(nextScheduledTime, notNullValue());
            assertThat(nextScheduledTime, greaterThanOrEqualTo(beforeScheduleNow + FREQUENCY.millis()));
            assertThat(nextScheduledTime, lessThanOrEqualTo(afterScheduleNow + FREQUENCY.millis()));
        }, 10, TimeUnit.SECONDS);
    }

    private TransformScheduler createStartAndRegisterTransform(String transformId) throws Exception {
        createSourceIndexWithMapping();
        indexDoc();

        TransformConfig config = TransformConfig.builder()
            .setId(transformId)
            .setSource(new SourceConfig(new String[] { SOURCE_INDEX }, QueryConfig.matchAll(), Map.of(), null))
            .setDest(new DestConfig(transformId + "-dest", null, null))
            .setFrequency(FREQUENCY)
            .setSyncConfig(new TimeSyncConfig("time", SYNC_DELAY))
            .setLatestConfig(new LatestConfig(List.of("key"), "time"))
            .build();
        createTransform(config);

        client().execute(StartTransformAction.INSTANCE, new StartTransformAction.Request(transformId, null, TimeValue.THIRTY_SECONDS))
            .actionGet(TimeValue.THIRTY_SECONDS);

        TransformScheduler scheduler = getInstanceFromNode(TransformServices.class).scheduler();
        assertBusy(() -> assertThat(scheduler.getNextScheduledTimeMillis(transformId), notNullValue()), 10, TimeUnit.SECONDS);
        return scheduler;
    }

    private void scheduleNowTransform(String transformId, boolean defer) {
        client().execute(
            ScheduleNowTransformAction.INSTANCE,
            new ScheduleNowTransformAction.Request(transformId, TimeValue.THIRTY_SECONDS, defer)
        ).actionGet(TimeValue.THIRTY_SECONDS);
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

    private void indexDoc() {
        client().bulk(
            new BulkRequest().add(
                new IndexRequest(SOURCE_INDEX).source(Map.of("time", Instant.now().toEpochMilli(), "key", randomAlphaOfLength(5)))
            )
        ).actionGet(TimeValue.THIRTY_SECONDS);
    }
}
