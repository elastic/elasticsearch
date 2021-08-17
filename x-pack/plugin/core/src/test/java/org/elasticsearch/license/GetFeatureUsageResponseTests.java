/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReferenceStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.license.GetFeatureUsageResponse.FeatureUsageInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class GetFeatureUsageResponseTests extends ESTestCase {

    public void assertStreamInputOutput(Version version, String family, String context) throws IOException {
        ZonedDateTime zdt = ZonedDateTime.now();
        FeatureUsageInfo fui = new FeatureUsageInfo(family, "feature", zdt, context, "gold");
        GetFeatureUsageResponse originalResponse = new GetFeatureUsageResponse(List.of(fui));
        BytesStreamOutput output = new BytesStreamOutput();
        output.setVersion(version);
        originalResponse.writeTo(output);

        BytesReferenceStreamInput input = new BytesReferenceStreamInput(output.bytes());
        input.setVersion(version);
        GetFeatureUsageResponse finalResponse = new GetFeatureUsageResponse(input);
        assertThat(finalResponse.getFeatures(), hasSize(1));
        FeatureUsageInfo fui2 = finalResponse.getFeatures().get(0);
        assertThat(fui2.family, equalTo(family));
        assertThat(fui2.name, equalTo("feature"));
        // time is truncated to nearest second
        assertThat(fui2.lastUsedTime, equalTo(zdt.withZoneSameInstant(ZoneOffset.UTC).withNano(0)));
        assertThat(fui2.context, equalTo(context));
        assertThat(fui2.licenseLevel, equalTo("gold"));
    }

    public void testPre715StreamFormat() throws IOException {
        assertStreamInputOutput(VersionUtils.getPreviousVersion(Version.V_7_15_0), null, null);
    }

    public void testStreamFormat() throws IOException {
        assertStreamInputOutput(Version.CURRENT, "family", "context");
        // family and context are optional
        assertStreamInputOutput(Version.CURRENT, null, null);
    }
}
