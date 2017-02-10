/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.graph;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.XPackFeatureSet;
import org.junit.Before;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GraphFeatureSetTests extends ESTestCase {

    private XPackLicenseState licenseState;

    @Before
    public void init() throws Exception {
        licenseState = mock(XPackLicenseState.class);
    }

    public void testAvailable() throws Exception {
        GraphFeatureSet featureSet = new GraphFeatureSet(Settings.EMPTY, licenseState);
        boolean available = randomBoolean();
        when(licenseState.isGraphAllowed()).thenReturn(available);
        assertThat(featureSet.available(), is(available));
        assertThat(featureSet.usage().available(), is(available));

        BytesStreamOutput out = new BytesStreamOutput();
        featureSet.usage().writeTo(out);
        XPackFeatureSet.Usage serializedUsage = new GraphFeatureSet.Usage(out.bytes().streamInput());
        assertThat(serializedUsage.available(), is(available));
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
        GraphFeatureSet featureSet = new GraphFeatureSet(settings.build(), licenseState);
        assertThat(featureSet.enabled(), is(enabled));
        assertThat(featureSet.usage().enabled(), is(enabled));

        BytesStreamOutput out = new BytesStreamOutput();
        featureSet.usage().writeTo(out);
        XPackFeatureSet.Usage serializedUsage = new GraphFeatureSet.Usage(out.bytes().streamInput());
        assertThat(serializedUsage.enabled(), is(enabled));
    }

}
