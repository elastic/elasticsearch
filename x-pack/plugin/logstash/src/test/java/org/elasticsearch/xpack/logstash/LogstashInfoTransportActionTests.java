/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.logstash;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
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

    public void testEnabledDefault() throws Exception {
        LogstashInfoTransportAction featureSet = new LogstashInfoTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            null
        );
        assertThat(featureSet.enabled(), is(true));

        LogstashUsageTransportAction usageAction = newUsageAction(false);
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        usageAction.masterOperation(null, null, null, future);
        XPackFeatureSet.Usage usage = future.get().getUsage();

        BytesStreamOutput out = new BytesStreamOutput();
        usage.writeTo(out);
        XPackFeatureSet.Usage serializedUsage = new LogstashFeatureSetUsage(out.bytes().streamInput());
        assertThat(serializedUsage.enabled(), is(true));
    }

    public void testAvailable() throws Exception {
        final XPackLicenseState licenseState = mock(XPackLicenseState.class);
        LogstashInfoTransportAction featureSet = new LogstashInfoTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            licenseState
        );
        boolean available = randomBoolean();
        when(licenseState.isAllowed(XPackLicenseState.Feature.LOGSTASH)).thenReturn(available);
        assertThat(featureSet.available(), is(available));

        var usageAction = newUsageAction(available);
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        usageAction.masterOperation(null, null, null, future);
        XPackFeatureSet.Usage usage = future.get().getUsage();
        assertThat(usage.available(), is(available));

        BytesStreamOutput out = new BytesStreamOutput();
        usage.writeTo(out);
        XPackFeatureSet.Usage serializedUsage = new LogstashFeatureSetUsage(out.bytes().streamInput());
        assertThat(serializedUsage.available(), is(available));
    }

    private LogstashUsageTransportAction newUsageAction(boolean available) {
        XPackLicenseState licenseState = mock(XPackLicenseState.class);
        when(licenseState.isAllowed(XPackLicenseState.Feature.LOGSTASH)).thenReturn(available);
        return new LogstashUsageTransportAction(mock(TransportService.class), null, null, mock(ActionFilters.class), null, licenseState);
    }
}
