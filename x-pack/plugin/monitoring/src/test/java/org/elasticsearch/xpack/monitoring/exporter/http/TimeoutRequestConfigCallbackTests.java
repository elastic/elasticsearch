/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.http.client.config.RequestConfig;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import org.junit.Before;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests {@link TimeoutRequestConfigCallback}.
 */
public class TimeoutRequestConfigCallbackTests extends ESTestCase {

    private final TimeValue connectTimeout = mock(TimeValue.class);
    private final int connectTimeoutMillis = randomInt();
    private final TimeValue socketTimeout = mock(TimeValue.class);
    private final int socketTimeoutMillis = randomInt();
    private final RequestConfig.Builder builder = mock(RequestConfig.Builder.class);

    @Before
    public void configureTimeouts() {
        when(connectTimeout.millis()).thenReturn((long)connectTimeoutMillis);
        when(socketTimeout.millis()).thenReturn((long)socketTimeoutMillis);
    }

    public void testCustomizeRequestConfig() {
        final TimeoutRequestConfigCallback callback = new TimeoutRequestConfigCallback(connectTimeout, socketTimeout);

        assertSame(builder, callback.customizeRequestConfig(builder));

        verify(builder).setConnectTimeout(connectTimeoutMillis);
        verify(builder).setSocketTimeout(socketTimeoutMillis);
    }

    public void testCustomizeRequestConfigWithOptionalParameters() {
        final TimeValue optionalConnectTimeout = randomFrom(connectTimeout, null);
        // avoid making both null at the same time
        final TimeValue optionalSocketTimeout = optionalConnectTimeout != null ? randomFrom(socketTimeout, null) : socketTimeout;

        final TimeoutRequestConfigCallback callback = new TimeoutRequestConfigCallback(optionalConnectTimeout, optionalSocketTimeout);

        assertSame(builder, callback.customizeRequestConfig(builder));
        assertSame(optionalConnectTimeout, callback.getConnectTimeout());
        assertSame(optionalSocketTimeout, callback.getSocketTimeout());

        if (optionalConnectTimeout != null) {
            verify(builder).setConnectTimeout(connectTimeoutMillis);
        } else {
            verify(builder, never()).setConnectTimeout(anyInt());
        }

        if (optionalSocketTimeout != null) {
            verify(builder).setSocketTimeout(socketTimeoutMillis);
        } else {
            verify(builder, never()).setSocketTimeout(anyInt());
        }
    }

}
