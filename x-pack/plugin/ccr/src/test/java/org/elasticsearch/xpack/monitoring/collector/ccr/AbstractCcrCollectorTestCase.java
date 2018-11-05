/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.ccr;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.monitoring.BaseCollectorTestCase;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public abstract class AbstractCcrCollectorTestCase extends BaseCollectorTestCase {

    public void testShouldCollectReturnsFalseIfMonitoringNotAllowed() {
        final Settings settings = randomFrom(ccrEnabledSettings(), ccrDisabledSettings());
        final boolean ccrAllowed = randomBoolean();
        final boolean isElectedMaster = randomBoolean();
        whenLocalNodeElectedMaster(isElectedMaster);

        // this controls the blockage
        when(licenseState.isMonitoringAllowed()).thenReturn(false);
        when(licenseState.isCcrAllowed()).thenReturn(ccrAllowed);

        final AbstractCcrCollector collector = createCollector(settings, clusterService, licenseState, client);

        assertThat(collector.shouldCollect(isElectedMaster), is(false));
        if (isElectedMaster) {
            verify(licenseState).isMonitoringAllowed();
        }
    }

    public void testShouldCollectReturnsFalseIfNotMaster() {
        // regardless of CCR being enabled
        final Settings settings = randomFrom(ccrEnabledSettings(), ccrDisabledSettings());

        when(licenseState.isMonitoringAllowed()).thenReturn(randomBoolean());
        when(licenseState.isCcrAllowed()).thenReturn(randomBoolean());
        // this controls the blockage
        final boolean isElectedMaster = false;

        final AbstractCcrCollector collector = createCollector(settings, clusterService, licenseState, client);

        assertThat(collector.shouldCollect(isElectedMaster), is(false));
    }

    public void testShouldCollectReturnsFalseIfCCRIsDisabled() {
        // this is controls the blockage
        final Settings settings = ccrDisabledSettings();

        when(licenseState.isMonitoringAllowed()).thenReturn(randomBoolean());
        when(licenseState.isCcrAllowed()).thenReturn(randomBoolean());

        final boolean isElectedMaster = randomBoolean();
        whenLocalNodeElectedMaster(isElectedMaster);

        final AbstractCcrCollector collector = createCollector(settings, clusterService, licenseState, client);

        assertThat(collector.shouldCollect(isElectedMaster), is(false));

        if (isElectedMaster) {
            verify(licenseState).isMonitoringAllowed();
        }
    }

    public void testShouldCollectReturnsFalseIfCCRIsNotAllowed() {
        final Settings settings = randomFrom(ccrEnabledSettings(), ccrDisabledSettings());

        when(licenseState.isMonitoringAllowed()).thenReturn(randomBoolean());
        // this is controls the blockage
        when(licenseState.isCcrAllowed()).thenReturn(false);
        final boolean isElectedMaster = randomBoolean();
        whenLocalNodeElectedMaster(isElectedMaster);

        final AbstractCcrCollector collector = createCollector(settings, clusterService, licenseState, client);

        assertThat(collector.shouldCollect(isElectedMaster), is(false));

        if (isElectedMaster) {
            verify(licenseState).isMonitoringAllowed();
        }
    }

    public void testShouldCollectReturnsTrue() {
        final Settings settings = ccrEnabledSettings();

        when(licenseState.isMonitoringAllowed()).thenReturn(true);
        when(licenseState.isCcrAllowed()).thenReturn(true);
        final boolean isElectedMaster = true;

        final AbstractCcrCollector collector = createCollector(settings, clusterService, licenseState, client);

        assertThat(collector.shouldCollect(isElectedMaster), is(true));

        verify(licenseState).isMonitoringAllowed();
    }

    abstract AbstractCcrCollector createCollector(Settings settings,
                                                  ClusterService clusterService,
                                                  XPackLicenseState licenseState,
                                                  Client client);

    private Settings ccrEnabledSettings() {
        // since it's the default, we want to ensure we test both with/without it
        return randomBoolean() ? Settings.EMPTY : Settings.builder().put(XPackSettings.CCR_ENABLED_SETTING.getKey(), true).build();
    }

    private Settings ccrDisabledSettings() {
        return Settings.builder().put(XPackSettings.CCR_ENABLED_SETTING.getKey(), false).build();
    }

}
