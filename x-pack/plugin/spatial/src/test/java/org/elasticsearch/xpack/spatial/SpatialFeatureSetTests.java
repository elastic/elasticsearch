/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.spatial.SpatialFeatureSetUsage;
import org.junit.Before;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SpatialFeatureSetTests extends ESTestCase {
    private XPackLicenseState licenseState;

    @Before
    public void init() throws Exception {
        licenseState = mock(XPackLicenseState.class);
    }

    public void testAvailable() throws Exception {
        SpatialFeatureSet featureSet = new SpatialFeatureSet(Settings.EMPTY, licenseState);
        boolean available = randomBoolean();
        when(licenseState.isSpatialAllowed()).thenReturn(available);
        assertThat(featureSet.available(), is(available));
        PlainActionFuture<XPackFeatureSet.Usage> future = new PlainActionFuture<>();
        featureSet.usage(future);
        XPackFeatureSet.Usage usage = future.get();
        assertThat(usage.available(), is(available));

        BytesStreamOutput out = new BytesStreamOutput();
        usage.writeTo(out);
        XPackFeatureSet.Usage serializedUsage = new SpatialFeatureSetUsage(out.bytes().streamInput());
        assertThat(serializedUsage.available(), is(available));
    }

    public void testEnabled() throws Exception {
        boolean enabled = true;
        Settings.Builder settings = Settings.builder();
        if (randomBoolean()) {
            settings.put("xpack.spatial.enabled", enabled);
        }
        SpatialFeatureSet featureSet = new SpatialFeatureSet(settings.build(), licenseState);
        assertThat(featureSet.enabled(), is(enabled));
        PlainActionFuture<XPackFeatureSet.Usage> future = new PlainActionFuture<>();
        featureSet.usage(future);
        XPackFeatureSet.Usage usage = future.get();
        assertThat(usage.enabled(), is(enabled));

        BytesStreamOutput out = new BytesStreamOutput();
        usage.writeTo(out);
        XPackFeatureSet.Usage serializedUsage = new SpatialFeatureSetUsage(out.bytes().streamInput());
        assertThat(serializedUsage.enabled(), is(enabled));
    }
}
