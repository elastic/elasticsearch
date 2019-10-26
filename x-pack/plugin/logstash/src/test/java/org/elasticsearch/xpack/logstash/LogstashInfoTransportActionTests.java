/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.logstash;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.logstash.LogstashFeatureSetUsage;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LogstashInfoTransportActionTests extends ESTestCase {

    public void testEnabledSetting() throws Exception {
        boolean enabled = randomBoolean();
        Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put("xpack.logstash.enabled", enabled)
            .build();
        LogstashInfoTransportAction featureSet = new LogstashInfoTransportAction(
            mock(TransportService.class), mock(ActionFilters.class), settings, null);
        assertThat(featureSet.enabled(), is(enabled));

        LogstashUsageTransportAction usageAction = newUsageAction(settings, false);
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        usageAction.masterOperation(null, null, null, future);
        XPackFeatureSet.Usage usage = future.get().getUsage();

        BytesStreamOutput out = new BytesStreamOutput();
        usage.writeTo(out);
        XPackFeatureSet.Usage serializedUsage = new LogstashFeatureSetUsage(out.bytes().streamInput());
        assertThat(serializedUsage.enabled(), is(enabled));
    }

    public void testEnabledDefault() throws Exception {
        Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        LogstashInfoTransportAction featureSet = new LogstashInfoTransportAction(
            mock(TransportService.class), mock(ActionFilters.class), settings, null);
        assertThat(featureSet.enabled(), is(true));
    }

    public void testAvailable() throws Exception {
        final XPackLicenseState licenseState = mock(XPackLicenseState.class);
        LogstashInfoTransportAction featureSet = new LogstashInfoTransportAction(
            mock(TransportService.class), mock(ActionFilters.class), Settings.EMPTY, licenseState);
        boolean available = randomBoolean();
        when(licenseState.isLogstashAllowed()).thenReturn(available);
        assertThat(featureSet.available(), is(available));

        var usageAction = newUsageAction(Settings.EMPTY, available);
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        usageAction.masterOperation(null, null, null, future);
        XPackFeatureSet.Usage usage = future.get().getUsage();
        assertThat(usage.available(), is(available));

        BytesStreamOutput out = new BytesStreamOutput();
        usage.writeTo(out);
        XPackFeatureSet.Usage serializedUsage = new LogstashFeatureSetUsage(out.bytes().streamInput());
        assertThat(serializedUsage.available(), is(available));
    }

    private LogstashUsageTransportAction newUsageAction(Settings settings, boolean available) {
        XPackLicenseState licenseState = mock(XPackLicenseState.class);
        when(licenseState.isLogstashAllowed()).thenReturn(available);
        return new LogstashUsageTransportAction(mock(TransportService.class), null,
            null, mock(ActionFilters.class), null, settings, licenseState);
    }
}
