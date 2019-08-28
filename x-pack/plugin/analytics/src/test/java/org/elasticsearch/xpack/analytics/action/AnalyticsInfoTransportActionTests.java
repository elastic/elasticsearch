/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics.action;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.analytics.AnalyticsFeatureSetUsage;
import org.junit.Before;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AnalyticsInfoTransportActionTests extends ESTestCase {

    private XPackLicenseState licenseState;

    @Before
    public void init() {
        licenseState = mock(XPackLicenseState.class);
    }

    public void testAvailable() throws Exception {
        AnalyticsInfoTransportAction featureSet = new AnalyticsInfoTransportAction(
            mock(TransportService.class), mock(ActionFilters.class), licenseState);
        boolean available = randomBoolean();
        when(licenseState.isDataScienceAllowed()).thenReturn(available);
        assertThat(featureSet.available(), is(available));

        AnalyticsUsageTransportAction usageAction = new AnalyticsUsageTransportAction(mock(TransportService.class), null, null,
            mock(ActionFilters.class), null, licenseState);
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        usageAction.masterOperation(null, null, null, future);
        XPackFeatureSet.Usage usage = future.get().getUsage();
        assertThat(usage.available(), is(available));

        BytesStreamOutput out = new BytesStreamOutput();
        usage.writeTo(out);
        XPackFeatureSet.Usage serializedUsage = new AnalyticsFeatureSetUsage(out.bytes().streamInput());
        assertThat(serializedUsage.available(), is(available));
    }

    public void testEnabled() throws Exception {
        AnalyticsInfoTransportAction featureSet = new AnalyticsInfoTransportAction(
            mock(TransportService.class), mock(ActionFilters.class), licenseState);
        assertThat(featureSet.enabled(), is(true));
        assertTrue(featureSet.enabled());

        AnalyticsUsageTransportAction usageAction = new AnalyticsUsageTransportAction(mock(TransportService.class),
            null, null, mock(ActionFilters.class), null, licenseState);
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        usageAction.masterOperation(null, null, null, future);
        XPackFeatureSet.Usage usage = future.get().getUsage();
        assertTrue(usage.enabled());

        BytesStreamOutput out = new BytesStreamOutput();
        usage.writeTo(out);
        XPackFeatureSet.Usage serializedUsage = new AnalyticsFeatureSetUsage(out.bytes().streamInput());
        assertTrue(serializedUsage.enabled());
    }

}
