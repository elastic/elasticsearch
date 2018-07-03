/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.actions.croneval;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.transport.actions.croneval.CronEvaluationAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.croneval.CronEvaluationRequest;
import org.elasticsearch.xpack.core.watcher.transport.actions.croneval.CronEvaluationResponse;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class TransportCronEvaluationActionTests extends ESTestCase {

    public void testCalculateTimestamps() {
        Instant now = Instant.now();
        List<String> timestamps = TransportCronEvaluationAction.calculateTimestamps(now.toEpochMilli(), "0 0 0 1 * ? 2099", 3);
        assertThat(timestamps, hasSize(3));
        assertThat(timestamps.get(0), is("Thu, 1 Jan 2099 00:00:00"));
        assertThat(timestamps.get(1), is("Sun, 1 Feb 2099 00:00:00"));
        assertThat(timestamps.get(2), is("Sun, 1 Mar 2099 00:00:00"));
    }

    public void testRequestSerialization() throws Exception {
        CronEvaluationRequest request = new CronEvaluationRequest();
        request.setExpression("test");
        int count = randomIntBetween(1, 100);
        boolean isCountSet = randomBoolean();
        if (isCountSet) {
            request.setCount(count);
        }

        BytesStreamOutput bytes = new BytesStreamOutput();
        request.writeTo(bytes);
        request = new CronEvaluationRequest();
        request.readFrom(bytes.bytes().streamInput());
        assertThat(request.getExpression(), is("test"));
        assertThat(request.getCount(), is(isCountSet ? count : 10));
    }

    public void testResponseSerialization() throws Exception {
        CronEvaluationResponse response = new CronEvaluationResponse(Arrays.asList("1", "2", "3"));
        BytesStreamOutput bytes = new BytesStreamOutput();
        response.writeTo(bytes);

        response = CronEvaluationAction.INSTANCE.newResponse();
        response.readFrom(bytes.bytes().streamInput());
        assertThat(response.getTimestamps(), contains("1", "2", "3"));
    }
}
