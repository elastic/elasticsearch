/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.clock;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.joda.time.DateTimeZone.UTC;

/**
 */
public class ClockTests extends ESTestCase {
    public void testNowUTC() {
        Clock clockMock = new ClockMock();
        assertThat(clockMock.now(UTC).getZone(), equalTo(UTC));
        assertThat(SystemClock.INSTANCE.now(UTC).getZone(), equalTo(UTC));
    }
}
