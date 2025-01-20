/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.analyses;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class OutlierDetectionTests extends AbstractBWCSerializationTestCase<OutlierDetection> {

    @Override
    protected OutlierDetection doParseInstance(XContentParser parser) throws IOException {
        return OutlierDetection.fromXContent(parser, false);
    }

    @Override
    protected OutlierDetection createTestInstance() {
        return createRandom();
    }

    @Override
    protected OutlierDetection mutateInstance(OutlierDetection instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    public static OutlierDetection createRandom() {
        Integer numberNeighbors = randomBoolean() ? null : randomIntBetween(1, 20);
        OutlierDetection.Method method = randomBoolean() ? null : randomFrom(OutlierDetection.Method.values());
        Double minScoreToWriteFeatureInfluence = randomBoolean() ? null : randomDoubleBetween(0.0, 1.0, true);
        return new OutlierDetection.Builder().setNNeighbors(numberNeighbors)
            .setMethod(method)
            .setFeatureInfluenceThreshold(minScoreToWriteFeatureInfluence)
            .setComputeFeatureInfluence(randomBoolean())
            .setOutlierFraction(randomDoubleBetween(0.0, 1.0, true))
            .setStandardizationEnabled(randomBoolean())
            .build();
    }

    public static OutlierDetection mutateForVersion(OutlierDetection instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected Writeable.Reader<OutlierDetection> instanceReader() {
        return OutlierDetection::new;
    }

    public void testGetParams_GivenDefaults() {
        OutlierDetection outlierDetection = new OutlierDetection.Builder().build();
        Map<String, Object> params = outlierDetection.getParams(null);
        assertThat(params.size(), equalTo(3));
        assertThat(params.containsKey("compute_feature_influence"), is(true));
        assertThat(params.get("compute_feature_influence"), is(true));
        assertThat(params.containsKey("outlier_fraction"), is(true));
        assertThat((double) params.get("outlier_fraction"), closeTo(0.05, 0.0001));
        assertThat(params.containsKey("standardization_enabled"), is(true));
        assertThat(params.get("standardization_enabled"), is(true));
    }

    public void testGetParams_GivenExplicitValues() {
        OutlierDetection outlierDetection = new OutlierDetection.Builder().setNNeighbors(42)
            .setMethod(OutlierDetection.Method.LDOF)
            .setFeatureInfluenceThreshold(0.42)
            .setComputeFeatureInfluence(false)
            .setOutlierFraction(0.9)
            .setStandardizationEnabled(false)
            .build();

        Map<String, Object> params = outlierDetection.getParams(null);

        assertThat(params.size(), equalTo(6));
        assertThat(params.get(OutlierDetection.N_NEIGHBORS.getPreferredName()), equalTo(42));
        assertThat(params.get(OutlierDetection.METHOD.getPreferredName()), equalTo(OutlierDetection.Method.LDOF));
        assertThat((Double) params.get(OutlierDetection.FEATURE_INFLUENCE_THRESHOLD.getPreferredName()), is(closeTo(0.42, 1E-9)));
        assertThat(params.get(OutlierDetection.COMPUTE_FEATURE_INFLUENCE.getPreferredName()), is(false));
        assertThat((Double) params.get(OutlierDetection.OUTLIER_FRACTION.getPreferredName()), is(closeTo(0.9, 1E-9)));
        assertThat(params.get(OutlierDetection.STANDARDIZATION_ENABLED.getPreferredName()), is(false));
    }

    public void testRequiredFieldsIsEmpty() {
        assertThat(createTestInstance().getRequiredFields(), is(empty()));
    }

    public void testFieldCardinalityLimitsIsEmpty() {
        assertThat(createTestInstance().getFieldCardinalityConstraints(), is(empty()));
    }

    public void testGetResultMappings() {
        Map<String, Object> mappedFields = createTestInstance().getResultMappings("test", null);
        assertThat(mappedFields.size(), equalTo(2));
        assertThat(mappedFields, hasKey("test.outlier_score"));
        assertThat(
            mappedFields.get("test.outlier_score"),
            equalTo(Collections.singletonMap("type", NumberFieldMapper.NumberType.DOUBLE.typeName()))
        );
        assertThat(mappedFields, hasKey("test.feature_influence"));
        assertThat(mappedFields.get("test.feature_influence"), equalTo(OutlierDetection.FEATURE_INFLUENCE_MAPPING));
    }

    public void testGetStateDocId() {
        OutlierDetection outlierDetection = createRandom();
        assertThat(outlierDetection.persistsState(), is(false));
        expectThrows(UnsupportedOperationException.class, () -> outlierDetection.getStateDocIdPrefix("foo"));
    }

    public void testInferenceConfig() {
        OutlierDetection outlierDetection = createRandom();
        assertThat(outlierDetection.inferenceConfig(null), is(nullValue()));
    }

    @Override
    protected OutlierDetection mutateInstanceForVersion(OutlierDetection instance, TransportVersion version) {
        return mutateForVersion(instance, version);
    }
}
