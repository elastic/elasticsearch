/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.watch;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.actions.ActionStatus;
import org.elasticsearch.xpack.core.watcher.actions.ActionStatus.AckStatus.State;
import org.elasticsearch.xpack.core.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.xpack.core.watcher.watch.WatchStatus;
import org.elasticsearch.xpack.watcher.actions.logging.LoggingAction;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class WatchStatusTests extends ESTestCase {

    public void testAckStatusIsResetOnUnmetCondition() {
        HashMap<String, ActionStatus> myMap = new HashMap<>();
        ActionStatus actionStatus = new ActionStatus(ZonedDateTime.now(ZoneOffset.UTC));
        myMap.put("foo", actionStatus);

        actionStatus.update(ZonedDateTime.now(ZoneOffset.UTC), new LoggingAction.Result.Success("foo"));
        actionStatus.onAck(ZonedDateTime.now(ZoneOffset.UTC));
        assertThat(actionStatus.ackStatus().state(), is(State.ACKED));

        WatchStatus status = new WatchStatus(ZonedDateTime.now(ZoneOffset.UTC), myMap);
        status.onCheck(false, ZonedDateTime.now(ZoneOffset.UTC));

        assertThat(status.actionStatus("foo").ackStatus().state(), is(State.AWAITS_SUCCESSFUL_EXECUTION));
    }

    public void testHeadersToXContent() throws Exception {
        WatchStatus status = new WatchStatus(ZonedDateTime.now(ZoneOffset.UTC), Collections.emptyMap());
        String key = randomAlphaOfLength(10);
        String value = randomAlphaOfLength(10);
        Map<String, String> headers = Collections.singletonMap(key, value);
        status.setHeaders(headers);

        // by default headers are hidden
        try (XContentBuilder builder = jsonBuilder()) {
            status.toXContent(builder, ToXContent.EMPTY_PARAMS);
            try (XContentParser parser = createParser(builder)) {
                Map<String, Object> fields = parser.map();
                assertThat(fields, not(hasKey(WatchStatus.Field.HEADERS.getPreferredName())));
            }
        }

        // but they are required when storing a watch
        try (XContentBuilder builder = jsonBuilder()) {
            status.toXContent(builder, WatcherParams.builder().hideHeaders(false).build());
            try (XContentParser parser = createParser(builder)) {
                parser.nextToken();
                Map<String, Object> fields = parser.map();
                assertThat(fields, hasKey(WatchStatus.Field.HEADERS.getPreferredName()));
                assertThat(fields.get(WatchStatus.Field.HEADERS.getPreferredName()), instanceOf(Map.class));
                Map<String, Object> extractedHeaders = (Map<String, Object>) fields.get(WatchStatus.Field.HEADERS.getPreferredName());
                assertThat(extractedHeaders, is(headers));
            }
        }
    }

    public void testHeadersSerialization() throws IOException {
        WatchStatus status = new WatchStatus(ZonedDateTime.now(ZoneOffset.UTC), Collections.emptyMap());
        String key = randomAlphaOfLength(10);
        String value = randomAlphaOfLength(10);
        Map<String, String> headers = Collections.singletonMap(key, value);
        status.setHeaders(headers);

        BytesStreamOutput out = new BytesStreamOutput();
        status.writeTo(out);
        BytesReference bytesReference = out.bytes();
        WatchStatus readStatus = new WatchStatus(bytesReference.streamInput());
        assertThat(readStatus, is(status));
        assertThat(readStatus.getHeaders(), is(headers));

        // test equals
        assertThat(readStatus.hashCode(), is(status.hashCode()));
        assertThat(readStatus, equalTo(status));
        readStatus.getHeaders().clear();
        assertThat(readStatus, not(equalTo(status)));
    }
}
