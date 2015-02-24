/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.throttle;

import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.ExecutionContext;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.elasticsearch.alerts.support.AlertsDateUtils.formatDate;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public class AckThrottlerTests extends ElasticsearchTestCase {

    @Test
    public void testWhenAcked() throws Exception {
        DateTime timestamp = new DateTime();
        ExecutionContext ctx = mock(ExecutionContext.class);
        Alert alert = mock(Alert.class);
        Alert.Status status = mock(Alert.Status.class);
        when(status.ackStatus()).thenReturn(new Alert.Status.AckStatus(Alert.Status.AckStatus.State.ACKED, timestamp));
        when(alert.status()).thenReturn(status);
        when(alert.name()).thenReturn("_alert");
        when(alert.acked()).thenReturn(true);
        when(ctx.alert()).thenReturn(alert);
        AckThrottler throttler = new AckThrottler();
        Throttler.Result result = throttler.throttle(ctx);
        assertThat(result.throttle(), is(true));
        assertThat(result.reason(), is("alert [_alert] was acked at [" + formatDate(timestamp) + "]"));
    }

    @Test
    public void testWhenNotAcked() throws Exception {
        DateTime timestamp = new DateTime();
        ExecutionContext ctx = mock(ExecutionContext.class);
        Alert alert = mock(Alert.class);
        Alert.Status status = mock(Alert.Status.class);
        Alert.Status.AckStatus.State state = randomFrom(Alert.Status.AckStatus.State.AWAITS_EXECUTION, Alert.Status.AckStatus.State.ACKABLE);
        when(status.ackStatus()).thenReturn(new Alert.Status.AckStatus(state, timestamp));
        when(alert.status()).thenReturn(status);
        when(alert.name()).thenReturn("_alert");
        when(alert.acked()).thenReturn(false);
        when(ctx.alert()).thenReturn(alert);
        AckThrottler throttler = new AckThrottler();
        Throttler.Result result = throttler.throttle(ctx);
        assertThat(result.throttle(), is(false));
        assertThat(result.reason(), nullValue());
    }
}
