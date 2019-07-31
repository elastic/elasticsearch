/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.spatial.SpatialFeatureSetUsage;
import org.junit.Before;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SpatialInfoTransportActionTests extends ESTestCase {

    private XPackLicenseState licenseState;

    @Before
    public void init() {
        licenseState = mock(XPackLicenseState.class);
    }

    public void testAvailable() throws Exception {
        SpatialInfoTransportAction featureSet = new SpatialInfoTransportAction(
            mock(TransportService.class), mock(ActionFilters.class), Settings.EMPTY, licenseState);
        boolean available = randomBoolean();
        when(licenseState.isSpatialAllowed()).thenReturn(available);
        assertThat(featureSet.available(), is(available));

        var usageAction = new SpatialUsageTransportAction(mock(TransportService.class), null, null,
            mock(ActionFilters.class), null, Settings.EMPTY, licenseState);
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        usageAction.masterOperation(null, null, null, future);
        XPackFeatureSet.Usage usage = future.get().getUsage();
        assertThat(usage.available(), is(available));

        BytesStreamOutput out = new BytesStreamOutput();
        usage.writeTo(out);
        XPackFeatureSet.Usage serializedUsage = new SpatialFeatureSetUsage(out.bytes().streamInput());
        assertThat(serializedUsage.available(), is(available));
    }

    public void testEnabled() throws Exception {
        Settings.Builder settings = Settings.builder();
        SpatialInfoTransportAction featureSet = new SpatialInfoTransportAction(
            mock(TransportService.class), mock(ActionFilters.class), settings.build(), licenseState);
        assertThat(featureSet.enabled(), is(true));
        assertTrue(featureSet.enabled());

        SpatialUsageTransportAction usageAction = new SpatialUsageTransportAction(mock(TransportService.class),
            null, null, mock(ActionFilters.class), null, settings.build(), licenseState);
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        usageAction.masterOperation(null, null, null, future);
        XPackFeatureSet.Usage usage = future.get().getUsage();
        assertTrue(usage.enabled());

        BytesStreamOutput out = new BytesStreamOutput();
        usage.writeTo(out);
        XPackFeatureSet.Usage serializedUsage = new SpatialFeatureSetUsage(out.bytes().streamInput());
        assertTrue(serializedUsage.enabled());
    }

}
