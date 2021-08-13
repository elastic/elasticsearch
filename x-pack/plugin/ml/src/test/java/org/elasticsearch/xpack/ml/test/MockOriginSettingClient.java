/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.test;


import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.mockito.Mockito;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * OriginSettingClient is a final class that cannot be mocked by mockito.
 * The solution is to wrap a non-mocked OriginSettingClient around a
 * mocked Client. All the mocking should take place on the client parameter.
 */
public class MockOriginSettingClient {

    /**
     * Create a OriginSettingClient on a mocked client.
     *
     * @param client The mocked client
     * @param origin Whatever
     * @return A OriginSettingClient using a mocked client
     */
    public static OriginSettingClient mockOriginSettingClient(Client client, String origin) {

        if (Mockito.mockingDetails(client).isMock() == false) {
            throw new AssertionError("client should be a mock");
        }
        ThreadContext tc = new ThreadContext(Settings.EMPTY);

        ThreadPool tp = mock(ThreadPool.class);
        when(tp.getThreadContext()).thenReturn(tc);

        when(client.threadPool()).thenReturn(tp);

        return new OriginSettingClient(client, origin);
    }
}
