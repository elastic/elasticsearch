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
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.spatial.SpatialFeatureSetUsage;
import org.elasticsearch.xpack.core.spatial.SpatialFeatureSetUsageTests;
import org.elasticsearch.xpack.core.spatial.action.SpatialStatsAction;
import org.junit.Before;

import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SpatialFeatureSetTests extends ESTestCase {
    private XPackLicenseState licenseState;
    private Client client;

    @Before
    public void init() {
        licenseState = mock(XPackLicenseState.class);
        client = mock(Client.class);
    }

    @SuppressWarnings("unchecked")
    public void testAvailable() throws Exception {
        SpatialFeatureSet featureSet = new SpatialFeatureSet(licenseState, client);
        boolean available = randomBoolean();
        when(licenseState.isAllowed(XPackLicenseState.Feature.SPATIAL)).thenReturn(available);
        doAnswer(invocationOnMock -> {
            ActionListener<SpatialStatsAction.Response> listener =
                (ActionListener<SpatialStatsAction.Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(SpatialFeatureSetUsageTests.randomStatsResponse());
            return null;
        }
        ).when(client).execute(eq(SpatialStatsAction.INSTANCE), any(), any());
        assertThat(featureSet.available(), is(available));
        PlainActionFuture<XPackFeatureSet.Usage> future = new PlainActionFuture<>();
        featureSet.usage(future);
        XPackFeatureSet.Usage usage = future.get();
        assertThat(usage.available(), is(available));

        BytesStreamOutput out = new BytesStreamOutput();
        usage.writeTo(out);
        XPackFeatureSet.Usage serializedUsage = new SpatialFeatureSetUsage(out.bytes().streamInput());
        assertThat(serializedUsage.available(), is(available));
    }

    @SuppressWarnings("unchecked")
    public void testEnabled() throws Exception {
        boolean enabled = true;
        Settings.Builder settings = Settings.builder();
        if (randomBoolean()) {
            settings.put("xpack.spatial.enabled", enabled);
        }
        SpatialFeatureSet featureSet = new SpatialFeatureSet(licenseState, client);
        assertThat(featureSet.enabled(), is(enabled));
        doAnswer(invocationOnMock -> {
                ActionListener<SpatialStatsAction.Response> listener =
                    (ActionListener<SpatialStatsAction.Response>) invocationOnMock.getArguments()[2];
                listener.onResponse(SpatialFeatureSetUsageTests.randomStatsResponse());
                return null;
            }
        ).when(client).execute(eq(SpatialStatsAction.INSTANCE), any(), any());
        PlainActionFuture<XPackFeatureSet.Usage> future = new PlainActionFuture<>();
        featureSet.usage(future);
        XPackFeatureSet.Usage usage = future.get();
        assertThat(usage.enabled(), is(enabled));

        BytesStreamOutput out = new BytesStreamOutput();
        usage.writeTo(out);
        XPackFeatureSet.Usage serializedUsage = new SpatialFeatureSetUsage(out.bytes().streamInput());
        assertThat(serializedUsage.enabled(), is(enabled));
    }
}
