/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.clock;

import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.elasticsearch.common.joda.time.DateTimeZone.UTC;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class ClockTests extends ElasticsearchTestCase {

    @Test
    public void test_now_UTC() {
        Clock clockMock = new ClockMock();
        assertThat(clockMock.now(UTC).getZone(), equalTo(UTC));
        assertThat(SystemClock.INSTANCE.now(UTC).getZone(), equalTo(UTC));
    }

}
