/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.rest.yaml.ObjectPath;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.monitoring.MonitoringFeatureSetUsage;
import org.elasticsearch.xpack.monitoring.exporter.Exporter;
import org.elasticsearch.xpack.monitoring.exporter.Exporters;
import org.elasticsearch.xpack.monitoring.exporter.http.HttpExporter;
import org.elasticsearch.xpack.monitoring.exporter.local.LocalExporter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MonitoringInfoTransportActionTests extends ESTestCase {

    private final MonitoringService monitoring = mock(MonitoringService.class);
    private final XPackLicenseState licenseState = mock(XPackLicenseState.class);
    private final Exporters exporters = mock(Exporters.class);

    public void testAvailable() {
        MonitoringInfoTransportAction featureSet = new MonitoringInfoTransportAction(
            mock(TransportService.class), mock(ActionFilters.class), licenseState);
        boolean available = randomBoolean();
        when(licenseState.isAllowed(XPackLicenseState.Feature.MONITORING)).thenReturn(available);
        assertThat(featureSet.available(), is(available));
    }

    public void testMonitoringEnabledByDefault() {
        MonitoringInfoTransportAction featureSet = new MonitoringInfoTransportAction(
            mock(TransportService.class), mock(ActionFilters.class), licenseState);
        assertThat(featureSet.enabled(), is(true));
    }

    public void testUsage() throws Exception {
        final Version serializedVersion = VersionUtils.randomCompatibleVersion(random(), Version.CURRENT);
        final boolean collectionEnabled = randomBoolean();
        int localCount = randomIntBetween(0, 5);
        List<Exporter> exporterList = new ArrayList<>();
        for (int i = 0; i < localCount; i++) {
            Exporter exporter = mockExporter(LocalExporter.TYPE, true);
            exporterList.add(exporter);
            if (randomBoolean()) {
                exporter = mockExporter(LocalExporter.TYPE, false);
                exporterList.add(exporter);
            }
        }
        int httpCount = randomIntBetween(0, 5);
        for (int i = 0; i < httpCount; i++) {
            Exporter exporter = mockExporter(HttpExporter.TYPE, true);
            exporterList.add(exporter);
            if (randomBoolean()) {
                exporter = mockExporter(HttpExporter.TYPE, false);
                exporterList.add(exporter);
            }
        }
        int xCount = randomIntBetween(0, 5);
        String xType = randomAlphaOfLength(10);
        for (int i = 0; i < xCount; i++) {
            Exporter exporter = mockExporter(xType, true);
            exporterList.add(exporter);
            if (randomBoolean()) {
                exporter = mockExporter(xType, false);
                exporterList.add(exporter);
            }
        }
        when(exporters.getEnabledExporters()).thenReturn(exporterList);
        when(monitoring.isMonitoringActive()).thenReturn(collectionEnabled);

        var usageAction = new MonitoringUsageTransportAction(mock(TransportService.class), null, null,
            mock(ActionFilters.class), null, licenseState,
            new MonitoringUsageServices(monitoring, exporters));
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        usageAction.masterOperation(null, null, null, future);
        MonitoringFeatureSetUsage monitoringUsage = (MonitoringFeatureSetUsage) future.get().getUsage();
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(serializedVersion);
        monitoringUsage.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        in.setVersion(serializedVersion);
        XPackFeatureSet.Usage serializedUsage = new MonitoringFeatureSetUsage(in);
        for (XPackFeatureSet.Usage usage : Arrays.asList(monitoringUsage, serializedUsage)) {
            ObjectPath  source;
            try (XContentBuilder builder = jsonBuilder()) {
                usage.toXContent(builder, ToXContent.EMPTY_PARAMS);
                source = ObjectPath.createFromXContent(builder.contentType().xContent(), BytesReference.bytes(builder));
            }
            assertThat(source.evaluate("collection_enabled"), is(collectionEnabled));
            assertThat(source.evaluate("enabled_exporters"), is(notNullValue()));
            if (localCount > 0) {
                assertThat(source.evaluate("enabled_exporters.local"), is(localCount));
            } else {
                assertThat(source.evaluate("enabled_exporters.local"), is(nullValue()));
            }
            if (httpCount > 0) {
                assertThat(source.evaluate("enabled_exporters.http"), is(httpCount));
            } else {
                assertThat(source.evaluate("enabled_exporters.http"), is(nullValue()));
            }
            if (xCount > 0) {
                assertThat(source.evaluate("enabled_exporters." + xType), is(xCount));
            } else {
                assertThat(source.evaluate("enabled_exporters." + xType), is(nullValue()));
            }
        }
    }

    private Exporter mockExporter(String type, boolean enabled) {
        Exporter exporter = mock(Exporter.class);
        Exporter.Config enabledConfig = mock(Exporter.Config.class);
        when(enabledConfig.enabled()).thenReturn(enabled);
        when(exporter.config()).thenReturn(enabledConfig);
        when(enabledConfig.type()).thenReturn(type);
        return exporter;
    }
}
