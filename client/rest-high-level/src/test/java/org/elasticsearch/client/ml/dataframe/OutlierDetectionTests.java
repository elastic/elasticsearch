/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml.dataframe;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class OutlierDetectionTests extends AbstractXContentTestCase<OutlierDetection> {

    public static OutlierDetection randomOutlierDetection() {
        return OutlierDetection.builder()
            .setNNeighbors(randomBoolean() ? null : randomIntBetween(1, 20))
            .setMethod(randomBoolean() ? null : randomFrom(OutlierDetection.Method.values()))
            .setFeatureInfluenceThreshold(randomBoolean() ? null : randomDoubleBetween(0.0, 1.0, true))
            .setComputeFeatureInfluence(randomBoolean() ? null : randomBoolean())
            .setOutlierFraction(randomBoolean() ? null : randomDoubleBetween(0.0, 1.0, true))
            .setStandardizationEnabled(randomBoolean() ? null : randomBoolean())
            .build();
    }

    @Override
    protected OutlierDetection doParseInstance(XContentParser parser) throws IOException {
        return OutlierDetection.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected OutlierDetection createTestInstance() {
        return randomOutlierDetection();
    }

    public void testGetParams_GivenDefaults() {
        OutlierDetection outlierDetection = OutlierDetection.createDefault();
        assertNull(outlierDetection.getNNeighbors());
        assertNull(outlierDetection.getMethod());
        assertNull(outlierDetection.getFeatureInfluenceThreshold());
        assertNull(outlierDetection.getComputeFeatureInfluence());
        assertNull(outlierDetection.getOutlierFraction());
        assertNull(outlierDetection.getStandardizationEnabled());
    }

    public void testGetParams_GivenExplicitValues() {
        OutlierDetection outlierDetection =
            OutlierDetection.builder()
                .setNNeighbors(42)
                .setMethod(OutlierDetection.Method.LDOF)
                .setFeatureInfluenceThreshold(0.5)
                .setComputeFeatureInfluence(true)
                .setOutlierFraction(0.42)
                .setStandardizationEnabled(false)
                .build();
        assertThat(outlierDetection.getNNeighbors(), equalTo(42));
        assertThat(outlierDetection.getMethod(), equalTo(OutlierDetection.Method.LDOF));
        assertThat(outlierDetection.getFeatureInfluenceThreshold(), closeTo(0.5, 1E-9));
        assertThat(outlierDetection.getComputeFeatureInfluence(), is(true));
        assertThat(outlierDetection.getOutlierFraction(), closeTo(0.42, 1E-9));
        assertThat(outlierDetection.getStandardizationEnabled(), is(false));
    }
}
