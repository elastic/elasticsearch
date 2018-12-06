/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.watch.clock;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.watch.ClockMock;
import org.joda.time.DateTime;

import java.time.Clock;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.joda.time.DateTimeZone.UTC;

public class ClockTests extends ESTestCase {
    public void testNowUTC() {
        Clock clockMock = ClockMock.frozen();
        assertThat(new DateTime(clockMock.millis(), UTC).getZone(), equalTo(UTC));
        assertThat(new DateTime(Clock.systemUTC().millis(), UTC).getZone(), equalTo(UTC));
    }

    public void testFreezeUnfreeze() throws Exception {
        ClockMock clockMock = ClockMock.frozen();
        final long millis = clockMock.millis();
        for (int i = 0; i < 10; i++) {
            assertThat(clockMock.millis(), equalTo(millis));
        }
        clockMock.unfreeze();
        assertBusy(() -> assertThat(clockMock.millis(), greaterThan(millis)));
    }
}
