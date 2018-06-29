/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.actions.croneval;

import org.elasticsearch.test.ESTestCase;

import java.time.Instant;
import java.util.List;

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
}
