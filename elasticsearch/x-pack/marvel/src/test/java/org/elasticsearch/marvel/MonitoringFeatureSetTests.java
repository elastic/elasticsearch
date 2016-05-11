/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.agent.exporter.Exporters;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 *
 */
public class MonitoringFeatureSetTests extends ESTestCase {

    private MonitoringLicensee licensee;
    private NamedWriteableRegistry namedWriteableRegistry;
    private Exporters exporters;

    @Before
    public void init() throws Exception {
        licensee = mock(MonitoringLicensee.class);
        exporters = mock(Exporters.class);
        namedWriteableRegistry = mock(NamedWriteableRegistry.class);
    }

    public void testWritableRegistration() throws Exception {
        new MonitoringFeatureSet(Settings.EMPTY, licensee, exporters, namedWriteableRegistry);
        verify(namedWriteableRegistry).register(eq(MonitoringFeatureSet.Usage.class), eq("xpack.usage.monitoring"), anyObject());
    }

    public void testAvailable() throws Exception {
        MonitoringFeatureSet featureSet = new MonitoringFeatureSet(Settings.EMPTY, licensee, exporters, namedWriteableRegistry);
        boolean available = randomBoolean();
        when(licensee.available()).thenReturn(available);
        assertThat(featureSet.available(), is(available));
    }

    public void testEnabled() throws Exception {
        boolean enabled = randomBoolean();
        Settings.Builder settings = Settings.builder();
        if (enabled) {
            if (randomBoolean()) {
                settings.put("xpack.monitoring.enabled", enabled);
            }
        } else {
            settings.put("xpack.monitoring.enabled", enabled);
        }
        MonitoringFeatureSet featureSet = new MonitoringFeatureSet(settings.build(), licensee, exporters, namedWriteableRegistry);
        assertThat(featureSet.enabled(), is(enabled));
    }
}
