/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.history;

import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.DateTimeZone;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.core.IsEqual.equalTo;

/**
 */
public class HistoryStoreTests extends ElasticsearchTestCase {

    @Test
    public void testIndexNameGeneration() {
        assertThat(HistoryStore.getAlertHistoryIndexNameForTime(new DateTime(0, DateTimeZone.UTC)), equalTo(".alert_history_1970-01-01"));
        assertThat(HistoryStore.getAlertHistoryIndexNameForTime(new DateTime(100000000000L, DateTimeZone.UTC)), equalTo(".alert_history_1973-03-03"));
        assertThat(HistoryStore.getAlertHistoryIndexNameForTime(new DateTime(1416582852000L, DateTimeZone.UTC)), equalTo(".alert_history_2014-11-21"));
        assertThat(HistoryStore.getAlertHistoryIndexNameForTime(new DateTime(2833165811000L, DateTimeZone.UTC)), equalTo(".alert_history_2059-10-12"));
    }

}
