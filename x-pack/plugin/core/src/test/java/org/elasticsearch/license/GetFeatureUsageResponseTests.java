/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.license.GetFeatureUsageResponse.FeatureUsageInfo;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class GetFeatureUsageResponseTests extends ESTestCase {

    public void assertStreamInputOutput(TransportVersion version, String family, String context) throws IOException {
        ZonedDateTime zdt = ZonedDateTime.now();
        FeatureUsageInfo fui = new FeatureUsageInfo(family, "feature", zdt, context, "gold");
        GetFeatureUsageResponse originalResponse = new GetFeatureUsageResponse(List.of(fui));
        BytesStreamOutput output = new BytesStreamOutput();
        output.setTransportVersion(version);
        originalResponse.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        input.setTransportVersion(version);
        GetFeatureUsageResponse finalResponse = new GetFeatureUsageResponse(input);
        assertThat(finalResponse.getFeatures(), hasSize(1));
        FeatureUsageInfo fui2 = finalResponse.getFeatures().get(0);
        assertThat(fui2.getFamily(), equalTo(family));
        assertThat(fui2.getName(), equalTo("feature"));
        // time is truncated to nearest second
        assertThat(fui2.getLastUsedTime(), equalTo(zdt.withZoneSameInstant(ZoneOffset.UTC).withNano(0)));
        assertThat(fui2.getContext(), equalTo(context));
        assertThat(fui2.getLicenseLevel(), equalTo("gold"));
    }

    public void testStreamFormat() throws IOException {
        assertStreamInputOutput(TransportVersion.current(), "family", "context");
        // family and context are optional
        assertStreamInputOutput(TransportVersion.current(), null, null);
    }
}
