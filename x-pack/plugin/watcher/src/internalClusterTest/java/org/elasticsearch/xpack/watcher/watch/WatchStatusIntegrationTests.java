/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.watch;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchResponse;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchRequestBuilder;
import org.elasticsearch.xpack.watcher.condition.NeverCondition;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.test.WatcherTestUtils;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;

import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.IntervalSchedule.Interval.Unit.SECONDS;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class WatchStatusIntegrationTests extends AbstractWatcherIntegrationTestCase {

    public void testThatStatusGetsUpdated() throws Exception {
        new PutWatchRequestBuilder(client(), "_name")
                .setSource(watchBuilder()
                        .trigger(schedule(interval(5, SECONDS)))
                        .input(simpleInput())
                        .condition(NeverCondition.INSTANCE)
                        .addAction("_logger", loggingAction("logged text")))
                .get();
        timeWarp().trigger("_name");

        GetWatchResponse getWatchResponse = new GetWatchRequestBuilder(client(), "_name").get();
        assertThat(getWatchResponse.isFound(), is(true));
        assertThat(getWatchResponse.getSource(), notNullValue());
        assertThat(getWatchResponse.getStatus().lastChecked(), is(notNullValue()));

        GetResponse getResponse = client().prepareGet().setIndex(".watches").setId("_name").get();
        getResponse.getSource();
        XContentSource source = new XContentSource(getResponse.getSourceAsBytesRef(), XContentType.JSON);

        String lastChecked = source.getValue("status.last_checked");
        assertThat(lastChecked, WatcherTestUtils.isSameDate(getWatchResponse.getStatus().lastChecked()));
        assertThat(getWatchResponse.getStatus().lastChecked(), isMillisResolution());
        // not started yet, so both nulls
        String lastMetCondition = source.getValue("status.last_met_condition");
        assertThat(lastMetCondition, is(nullValue()));
        assertThat(getWatchResponse.getStatus().lastMetCondition(), is(nullValue()));
    }

    private Matcher<ZonedDateTime> isMillisResolution() {
        return new FeatureMatcher<ZonedDateTime,Boolean>(equalTo(true), "has millisecond precision", "precission") {
            @Override
            protected Boolean featureValueOf(ZonedDateTime actual) {
                //if date has millisecond precision its nanosecond field will be rounded to millis (equal millis * 10^6)
                return actual.getNano() == actual.get(ChronoField.MILLI_OF_SECOND) * 1000_000;
            }
        };
    }

}
