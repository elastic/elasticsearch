/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.test.integration;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.protocol.xpack.watcher.PutWatchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.watcher.actions.ActionStatus;
import org.elasticsearch.xpack.core.watcher.client.WatchSourceBuilder;
import org.elasticsearch.xpack.core.watcher.history.HistoryStoreField;
import org.elasticsearch.xpack.core.watcher.history.WatchRecord;
import org.elasticsearch.xpack.core.watcher.input.Input;
import org.elasticsearch.xpack.core.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.watch.WatchStatus;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateRequest;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.test.WatcherTestUtils;
import org.elasticsearch.xpack.watcher.trigger.schedule.IntervalSchedule;

import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.chainInput;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.searchInput;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.templateRequest;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class HistoryIntegrationTests extends AbstractWatcherIntegrationTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {

        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("xpack.watcher.max.history.record.size", "100kb") // used for testThatHistoryIsTruncated()
            .build();
    }

    // issue: https://github.com/elastic/x-plugins/issues/2338
    public void testThatHistoryIsWrittenWithChainedInput() throws Exception {
        XContentBuilder xContentBuilder = jsonBuilder().startObject()
            .startObject("inner")
            .field("date", "2015-06-06")
            .endObject()
            .endObject();
        index("foo", "1", xContentBuilder);
        refresh();

        WatchSourceBuilder builder = watchBuilder().trigger(schedule(interval("10s"))).addAction("logging", loggingAction("foo"));

        builder.input(
            chainInput().add(
                "first",
                searchInput(templateRequest(searchSource().sort(SortBuilders.fieldSort("inner.date").order(SortOrder.DESC)), "foo"))
            )
        );

        PutWatchResponse response = new PutWatchRequestBuilder(client()).setId("test_watch").setSource(builder).get();
        assertThat(response.isCreated(), is(true));

        new ExecuteWatchRequestBuilder(client()).setId("test_watch").setRecordExecution(true).get();

        assertBusy(() -> { assertHitCount(getWatchHistory(), 1); });
    }

    // See https://github.com/elastic/x-plugins/issues/2913
    public void testFailedInputResultWithDotsInFieldNameGetsStored() throws Exception {
        WatcherSearchTemplateRequest request = templateRequest(
            searchSource().query(matchAllQuery()).sort("trigger_event.triggered_time", SortOrder.DESC).size(1),
            "non-existing-index"
        );

        // The result of the search input will be a failure, because a missing index does not exist when
        // the query is executed
        @SuppressWarnings({ "rawtypes" })
        Input.Builder<? extends Input> input = searchInput(request);
        // wrapping this randomly into a chained input to test this as well
        boolean useChained = randomBoolean();
        if (useChained) {
            input = chainInput().add("chained", input);
        }

        new PutWatchRequestBuilder(client()).setId("test_watch")
            .setSource(
                watchBuilder().trigger(schedule(interval(5, IntervalSchedule.Interval.Unit.HOURS)))
                    .input(input)
                    .addAction("_logger", loggingAction("#### randomLogging"))
            )
            .get();

        new ExecuteWatchRequestBuilder(client()).setId("test_watch").setRecordExecution(true).get();

        assertBusy(() -> { assertHitCount(getWatchHistory(), 1); });

        // as fields with dots are allowed in 5.0 again, the mapping must be checked in addition
        GetMappingsResponse response = indicesAdmin().prepareGetMappings(".watcher-history*").get();
        XContentSource source = new XContentSource(
            response.getMappings().values().iterator().next().source().uncompressed(),
            XContentType.JSON
        );
        // lets make sure the body fields are disabled
        if (useChained) {
            String chainedPath = SINGLE_MAPPING_NAME
                + ".properties.result.properties.input.properties.chain.properties.chained.properties.search"
                + ".properties.request.properties.body.enabled";
            assertThat(source.getValue(chainedPath), is(false));
        } else {
            String path = SINGLE_MAPPING_NAME
                + ".properties.result.properties.input.properties.search.properties.request.properties.body.enabled";
            assertThat(source.getValue(path), is(false));
        }
    }

    // See https://github.com/elastic/x-plugins/issues/2913
    public void testPayloadInputWithDotsInFieldNameWorks() throws Exception {

        Input.Builder<? extends Input> input = simpleInput("foo.bar", "bar");

        // wrapping this randomly into a chained input to test this as well
        boolean useChained = randomBoolean();
        if (useChained) {
            input = chainInput().add("chained", input);
        }

        new PutWatchRequestBuilder(client()).setId("test_watch")
            .setSource(
                watchBuilder().trigger(schedule(interval(5, IntervalSchedule.Interval.Unit.HOURS)))
                    .input(input)
                    .addAction("_logger", loggingAction("#### randomLogging"))
            )
            .get();

        new ExecuteWatchRequestBuilder(client()).setId("test_watch").setRecordExecution(true).get();

        assertBusy(() -> { assertHitCount(getWatchHistory(), 1); });

        // as fields with dots are allowed in 5.0 again, the mapping must be checked in addition
        GetMappingsResponse response = indicesAdmin().prepareGetMappings(".watcher-history*").get();
        XContentSource source = new XContentSource(
            response.getMappings().values().iterator().next().source().uncompressed(),
            XContentType.JSON
        );

        // lets make sure the body fields are disabled
        if (useChained) {
            String path = SINGLE_MAPPING_NAME
                + ".properties.result.properties.input.properties.chain.properties.chained.properties.payload.enabled";
            assertThat(source.getValue(path), is(false));
        } else {
            String path = SINGLE_MAPPING_NAME + ".properties.result.properties.input.properties.payload.enabled";
            assertThat(source.getValue(path), is(false));
        }
    }

    public void testThatHistoryContainsStatus() throws Exception {
        new PutWatchRequestBuilder(client()).setId("test_watch")
            .setSource(
                watchBuilder().trigger(schedule(interval(5, IntervalSchedule.Interval.Unit.HOURS)))
                    .input(simpleInput("foo", "bar"))
                    .addAction("_logger", loggingAction("#### randomLogging"))
            )
            .get();

        new ExecuteWatchRequestBuilder(client()).setId("test_watch").setRecordExecution(true).get();

        WatchStatus status = new GetWatchRequestBuilder(client()).setId("test_watch").get().getStatus();

        assertBusy(() -> {
            assertResponse(getWatchHistory(), searchResponse -> {
                assertHitCount(searchResponse, 1);
                SearchHit hit = searchResponse.getHits().getAt(0);

                XContentSource source = new XContentSource(hit.getSourceRef(), XContentType.JSON);

                Boolean active = source.getValue("status.state.active");
                assertThat(active, is(status.state().isActive()));

                String timestamp = source.getValue("status.state.timestamp");
                assertThat(timestamp, WatcherTestUtils.isSameDate(status.state().getTimestamp()));

                String lastChecked = source.getValue("status.last_checked");
                assertThat(lastChecked, WatcherTestUtils.isSameDate(status.lastChecked()));
                String lastMetCondition = source.getValue("status.last_met_condition");
                assertThat(lastMetCondition, WatcherTestUtils.isSameDate(status.lastMetCondition()));

                Integer version = source.getValue("status.version");
                int expectedVersion = (int) (status.version() - 1);
                assertThat(version, is(expectedVersion));

                ActionStatus actionStatus = status.actionStatus("_logger");
                String ackStatusState = source.getValue("status.actions._logger.ack.state").toString().toUpperCase(Locale.ROOT);
                assertThat(ackStatusState, is(actionStatus.ackStatus().state().toString()));

                Boolean lastExecutionSuccesful = source.getValue("status.actions._logger.last_execution.successful");
                assertThat(lastExecutionSuccesful, is(actionStatus.lastExecution().successful()));
            });
        });

        assertBusy(() -> {
            // also ensure that the status field is disabled in the watch history
            GetMappingsResponse response = indicesAdmin().prepareGetMappings(".watcher-history*").get();
            XContentSource mappingSource = new XContentSource(
                response.getMappings().values().iterator().next().source().uncompressed(),
                XContentType.JSON
            );
            assertThat(mappingSource.getValue(SINGLE_MAPPING_NAME + ".properties.status.enabled"), is(false));
            assertThat(mappingSource.getValue(SINGLE_MAPPING_NAME + ".properties.status.properties.status"), is(nullValue()));
            assertThat(
                mappingSource.getValue(SINGLE_MAPPING_NAME + ".properties.status.properties.status.properties.active"),
                is(nullValue())
            );
        });
    }

    public void testThatHistoryIsTruncated() throws Exception {
        {
            /*
             * The input for this watch is 20 KB, smaller than the configured 100 KB of HistoryStore's MAX_HISTORY_SIZE_SETTING. So we do
             * not expect its history record to be truncated.
             */
            new PutWatchRequestBuilder(client()).setId("test_watch_small")
                .setSource(
                    watchBuilder().trigger(schedule(interval(5, IntervalSchedule.Interval.Unit.HOURS)))
                        .input(simpleInput("foo", randomAlphaOfLength((int) ByteSizeValue.ofKb(20).getBytes())))
                        .addAction("_logger", loggingAction("#### randomLogging"))
                )
                .get();
            new ExecuteWatchRequestBuilder(client()).setId("test_watch_small").setRecordExecution(true).get();
            assertBusy(() -> {
                assertResponse(getWatchHistory(), searchResponse -> {
                    assertHitCount(searchResponse, 1);
                    SearchHit hit = searchResponse.getHits().getAt(0);
                    XContentSource source = new XContentSource(hit.getSourceRef(), XContentType.JSON);
                    Map<String, Object> input = source.getValue("input");
                    assertThat(input.containsKey(WatchRecord.TRUNCATED_RECORD_KEY), equalTo(false));
                    assertThat(input.containsKey("simple"), equalTo(true));
                    Map<String, Object> result = source.getValue("result");
                    assertThat(result.containsKey(WatchRecord.TRUNCATED_RECORD_KEY), equalTo(false));
                    assertThat(result.containsKey("input"), equalTo(true));
                    assertThat(result.containsKey("actions"), equalTo(true));
                    assertThat(result.containsKey("condition"), equalTo(true));
                });
            });
        }
        {
            /*
             * The input for this watch is 500 KB, much bigger than the configured 100 KB of HistoryStore's MAX_HISTORY_SIZE_SETTING. So we
             * expect to see its history record truncated before being stored.
             */
            new PutWatchRequestBuilder(client()).setId("test_watch_large")
                .setSource(
                    watchBuilder().trigger(schedule(interval(5, IntervalSchedule.Interval.Unit.HOURS)))
                        .input(simpleInput("foo", randomAlphaOfLength((int) ByteSizeValue.ofKb(500).getBytes())))
                        .addAction("_logger", loggingAction("#### randomLogging"))
                )
                .get();
            new ExecuteWatchRequestBuilder(client()).setId("test_watch_large").setRecordExecution(true).get();
            assertBusy(() -> {
                assertResponse(getWatchHistory(), searchResponse -> {
                    assertHitCount(searchResponse, 2);
                    SearchHit hit = searchResponse.getHits().getAt(1);
                    XContentSource source = new XContentSource(hit.getSourceRef(), XContentType.JSON);
                    Map<String, Object> input = source.getValue("input");
                    assertThat(input.containsKey(WatchRecord.TRUNCATED_RECORD_KEY), equalTo(true));
                    assertThat(input.get(WatchRecord.TRUNCATED_RECORD_KEY), equalTo(WatchRecord.TRUNCATED_RECORD_VALUE));
                    assertThat(input.containsKey("simple"), equalTo(false));
                    Map<String, Object> result = source.getValue("result");
                    assertThat(result.containsKey(WatchRecord.TRUNCATED_RECORD_KEY), equalTo(true));
                    assertThat(result.get(WatchRecord.TRUNCATED_RECORD_KEY), equalTo(WatchRecord.TRUNCATED_RECORD_VALUE));
                    assertThat(result.containsKey("input"), equalTo(false));
                    assertThat(result.containsKey("actions"), equalTo(false));
                    assertThat(result.containsKey("condition"), equalTo(false));
                });
            });
        }
    }

    /*
     * Returns a SearchRequestBuilder containing up to the default number of watch history records (10) if the .watcher-history* is ready.
     * Otherwise it throws an AssertionError.
     */
    private SearchRequestBuilder getWatchHistory() {
        ensureGreen(HistoryStoreField.DATA_STREAM);
        flushAndRefresh(".watcher-history-*");
        return prepareSearch(".watcher-history-*").addSort("@timestamp", SortOrder.ASC);
    }

}
