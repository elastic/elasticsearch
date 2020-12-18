/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.junit.Before;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

public class SpatialFeatureSetTests extends ESTestCase {
    private Client client;

    @Before
    public void init() {
        client = mock(Client.class);
    }

    @SuppressWarnings("unchecked")
    public void testAvailable() throws Exception {
        SpatialFeatureSet featureSet = new SpatialFeatureSet(client);
        PlainActionFuture<XPackFeatureSet.Usage> future = new PlainActionFuture<>();
        featureSet.usage(future);
        XPackFeatureSet.Usage usage = future.get();
        assertThat(usage.available(), is(true));
    }

    @SuppressWarnings("unchecked")
    public void testEnabled() throws Exception {
        SpatialFeatureSet featureSet = new SpatialFeatureSet(client);
        PlainActionFuture<XPackFeatureSet.Usage> future = new PlainActionFuture<>();
        featureSet.usage(future);
        XPackFeatureSet.Usage usage = future.get();
        assertThat(usage.enabled(), is(true));
    }
}
