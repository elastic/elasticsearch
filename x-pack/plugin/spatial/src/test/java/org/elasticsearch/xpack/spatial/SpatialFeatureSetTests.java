/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.spatial.SpatialFeatureSetUsageTests;
import org.elasticsearch.xpack.core.spatial.action.SpatialStatsAction;
import org.junit.Before;

import static org.hamcrest.core.Is.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class SpatialFeatureSetTests extends ESTestCase {
    private Client client;

    @Before
    public void init() {
        client = mock(Client.class);
    }

    @SuppressWarnings("unchecked")
    public void testAvailableAndEnabled() throws Exception {
        SpatialFeatureSet featureSet = new SpatialFeatureSet(client);
        doAnswer(invocationOnMock -> {
            ActionListener<SpatialStatsAction.Response> listener = (ActionListener<SpatialStatsAction.Response>) invocationOnMock
                .getArguments()[2];
            listener.onResponse(SpatialFeatureSetUsageTests.randomStatsResponse());
            return null;
        }).when(client).execute(eq(SpatialStatsAction.INSTANCE), any(), any());
        PlainActionFuture<XPackFeatureSet.Usage> future = new PlainActionFuture<>();
        featureSet.usage(future);
        XPackFeatureSet.Usage usage = future.get();
        assertThat(usage.available(), is(true));
        assertThat(usage.enabled(), is(true));
    }
}
