/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.graph;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.plugin.core.XPackLicenseState;
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
public class GraphFeatureSetTests extends ESTestCase {

    private XPackLicenseState licenseState;
    private NamedWriteableRegistry namedWriteableRegistry;

    @Before
    public void init() throws Exception {
        licenseState = mock(XPackLicenseState.class);
        namedWriteableRegistry = mock(NamedWriteableRegistry.class);
    }

    public void testWritableRegistration() throws Exception {
        new GraphFeatureSet(Settings.EMPTY, licenseState, namedWriteableRegistry);
        verify(namedWriteableRegistry).register(eq(GraphFeatureSet.Usage.class), eq("xpack.usage.graph"), anyObject());
    }

    public void testAvailable() throws Exception {
        GraphFeatureSet featureSet = new GraphFeatureSet(Settings.EMPTY, licenseState, namedWriteableRegistry);
        boolean available = randomBoolean();
        when(licenseState.isGraphAllowed()).thenReturn(available);
        assertThat(featureSet.available(), is(available));
    }

    public void testEnabled() throws Exception {
        boolean enabled = randomBoolean();
        Settings.Builder settings = Settings.builder();
        if (enabled) {
            if (randomBoolean()) {
                settings.put("xpack.graph.enabled", enabled);
            }
        } else {
            settings.put("xpack.graph.enabled", enabled);
        }
        GraphFeatureSet featureSet = new GraphFeatureSet(settings.build(), licenseState, namedWriteableRegistry);
        assertThat(featureSet.enabled(), is(enabled));
    }

}
