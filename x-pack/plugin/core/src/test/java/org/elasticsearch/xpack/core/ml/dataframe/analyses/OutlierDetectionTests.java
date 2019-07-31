/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.analyses;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class OutlierDetectionTests extends AbstractSerializingTestCase<OutlierDetection> {

    @Override
    protected OutlierDetection doParseInstance(XContentParser parser) throws IOException {
        return OutlierDetection.fromXContent(parser, false);
    }

    @Override
    protected OutlierDetection createTestInstance() {
        return createRandom();
    }

    public static OutlierDetection createRandom() {
        Integer numberNeighbors = randomBoolean() ? null : randomIntBetween(1, 20);
        OutlierDetection.Method method = randomBoolean() ? null : randomFrom(OutlierDetection.Method.values());
        Double minScoreToWriteFeatureInfluence = randomBoolean() ? null : randomDoubleBetween(0.0, 1.0, true);
        return new OutlierDetection(numberNeighbors, method, minScoreToWriteFeatureInfluence);
    }

    @Override
    protected Writeable.Reader<OutlierDetection> instanceReader() {
        return OutlierDetection::new;
    }

    public void testGetParams_GivenDefaults() {
        OutlierDetection outlierDetection = new OutlierDetection();
        assertThat(outlierDetection.getParams().isEmpty(), is(true));
    }

    public void testGetParams_GivenExplicitValues() {
        OutlierDetection outlierDetection = new OutlierDetection(42, OutlierDetection.Method.LDOF, 0.42);

        Map<String, Object> params = outlierDetection.getParams();

        assertThat(params.size(), equalTo(3));
        assertThat(params.get(OutlierDetection.N_NEIGHBORS.getPreferredName()), equalTo(42));
        assertThat(params.get(OutlierDetection.METHOD.getPreferredName()), equalTo(OutlierDetection.Method.LDOF));
        assertThat((Double) params.get(OutlierDetection.FEATURE_INFLUENCE_THRESHOLD.getPreferredName()),
            is(closeTo(0.42, 1E-9)));
    }
}
