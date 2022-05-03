/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.test.integration;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchRequestBuilder;
import org.elasticsearch.xpack.watcher.condition.CompareCondition;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateRequest;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.trigger.schedule.IntervalSchedule;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.searchInput;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.templateRequest;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class RejectedExecutionTests extends AbstractWatcherIntegrationTestCase {

    @Override
    protected boolean timeWarped() {
        // need to use the real scheduler
        return false;
    }

    public void testHistoryOnRejection() throws Exception {
        createIndex("idx");
        client().prepareIndex("idx").setSource("field", "a").get();
        refresh();
        WatcherSearchTemplateRequest request = templateRequest(searchSource().query(termQuery("field", "a")), "idx");
        new PutWatchRequestBuilder(client()).setId(randomAlphaOfLength(5))
            .setSource(
                watchBuilder().trigger(schedule(interval(1, IntervalSchedule.Interval.Unit.SECONDS)))
                    .input(searchInput(request))
                    .condition(new CompareCondition("ctx.payload.hits.total", CompareCondition.Op.EQ, 1L))
                    .addAction("_logger", loggingAction("_logging").setCategory("_category"))
            )
            .get();

        assertBusy(() -> {
            flushAndRefresh(".watcher-history-*");
            SearchResponse searchResponse = client().prepareSearch(".watcher-history-*").get();
            assertThat(searchResponse.getHits().getTotalHits().value, greaterThanOrEqualTo(2L));
        });
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {

        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
            .put(LicenseService.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial")
            .put("thread_pool.write.size", 1)
            .put("thread_pool.write.queue_size", 1)
            .put("xpack.watcher.thread_pool.size", 1)
            .put("xpack.watcher.thread_pool.queue_size", 0)
            .build();
    }

}
