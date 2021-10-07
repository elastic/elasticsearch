/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.logstash;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.logstash.LogstashFeatureSetUsage;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LogstashFeatureSetTests extends ESTestCase {

    public void testEnabledDefault() throws Exception {
        LogstashFeatureSet featureSet = new LogstashFeatureSet(null);
        assertThat(featureSet.enabled(), is(true));
    }

    public void testAvailable() throws Exception {
        final MockLicenseState licenseState = mock(MockLicenseState.class);
        LogstashFeatureSet featureSet = new LogstashFeatureSet(licenseState);
        boolean available = randomBoolean();
        when(licenseState.isAllowed(Logstash.LOGSTASH_FEATURE)).thenReturn(available);
        assertThat(featureSet.available(), is(available));

        PlainActionFuture<XPackFeatureSet.Usage> future = new PlainActionFuture<>();
        featureSet.usage(future);
        XPackFeatureSet.Usage usage = future.get();
        assertThat(usage.available(), is(available));

        BytesStreamOutput out = new BytesStreamOutput();
        usage.writeTo(out);
        XPackFeatureSet.Usage serializedUsage = new LogstashFeatureSetUsage(out.bytes().streamInput());
        assertThat(serializedUsage.available(), is(available));
    }
}
