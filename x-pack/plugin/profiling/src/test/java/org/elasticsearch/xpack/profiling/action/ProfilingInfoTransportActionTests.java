/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

public class ProfilingInfoTransportActionTests extends ESTestCase {
    public void testAvailable() {
        // trial mode - allow feature
        XPackLicenseState licenseState = new XPackLicenseState(() -> 0);

        boolean enabled = randomBoolean();
        Settings settings = Settings.builder().put(XPackSettings.PROFILING_ENABLED.getKey(), enabled).build();

        ProfilingInfoTransportAction featureSet = new ProfilingInfoTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            settings,
            new ProfilingLicenseChecker(() -> licenseState)
        );
        assertThat(featureSet.available(), is(true));
        assertThat(featureSet.enabled(), is(enabled));
    }

    public void testUnavailable() {
        // won't work in BASIC
        XPackLicenseState licenseState = new XPackLicenseState(() -> 0, new XPackLicenseStatus(License.OperationMode.BASIC, true, null));

        boolean enabled = randomBoolean();
        Settings settings = Settings.builder().put(XPackSettings.PROFILING_ENABLED.getKey(), enabled).build();

        ProfilingInfoTransportAction featureSet = new ProfilingInfoTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            settings,
            new ProfilingLicenseChecker(() -> licenseState)
        );
        assertThat(featureSet.available(), is(false));
        assertThat(featureSet.enabled(), is(enabled));
    }
}
