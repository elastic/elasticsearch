/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.rollup.RollupFeatureSetUsage;
import org.junit.Before;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RollupInfoTransportActionTests extends ESTestCase {
    private XPackLicenseState licenseState;

    @Before
    public void init() {
        licenseState = mock(XPackLicenseState.class);
    }

    public void testAvailable() {
        RollupInfoTransportAction featureSet = new RollupInfoTransportAction(
            mock(TransportService.class), mock(ActionFilters.class), Settings.EMPTY, licenseState);
        boolean available = randomBoolean();
        when(licenseState.isRollupAllowed()).thenReturn(available);
        assertThat(featureSet.available(), is(available));
    }

    public void testEnabledSetting() {
        boolean enabled = randomBoolean();
        Settings.Builder settings = Settings.builder();
        settings.put("xpack.rollup.enabled", enabled);
        RollupInfoTransportAction featureSet = new RollupInfoTransportAction(
            mock(TransportService.class), mock(ActionFilters.class), settings.build(), licenseState);
        assertThat(featureSet.enabled(), is(enabled));
    }

    public void testEnabledDefault() {
        RollupInfoTransportAction featureSet = new RollupInfoTransportAction(
            mock(TransportService.class), mock(ActionFilters.class), Settings.EMPTY, licenseState);
        assertThat(featureSet.enabled(), is(true));
    }

    public void testUsage() throws ExecutionException, InterruptedException, IOException {
        var usageAction = new RollupUsageTransportAction(mock(TransportService.class), null, null,
            mock(ActionFilters.class), null, Settings.EMPTY, licenseState);
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        usageAction.masterOperation(null, null, null, future);
        XPackFeatureSet.Usage rollupUsage = future.get().getUsage();
        BytesStreamOutput out = new BytesStreamOutput();
        rollupUsage.writeTo(out);
        XPackFeatureSet.Usage serializedUsage = new RollupFeatureSetUsage(out.bytes().streamInput());
        assertThat(rollupUsage.name(), is(serializedUsage.name()));
        assertThat(rollupUsage.enabled(), is(serializedUsage.enabled()));
    }

}
