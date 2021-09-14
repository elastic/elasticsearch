/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.job.results;

import org.elasticsearch.client.ml.job.config.DetectorFunction;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class AnomalyCauseTests extends AbstractXContentTestCase<AnomalyCause> {

    @Override
    protected AnomalyCause createTestInstance() {
        AnomalyCause anomalyCause = new AnomalyCause();
        if (randomBoolean()) {
            int size = randomInt(10);
            List<Double> actual = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                actual.add(randomDouble());
            }
            anomalyCause.setActual(actual);
        }
        if (randomBoolean()) {
            int size = randomInt(10);
            List<Double> typical = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                typical.add(randomDouble());
            }
            anomalyCause.setTypical(typical);
        }
        if (randomBoolean()) {
            anomalyCause.setByFieldName(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            anomalyCause.setByFieldValue(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            anomalyCause.setCorrelatedByFieldValue(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            anomalyCause.setOverFieldName(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            anomalyCause.setOverFieldValue(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            anomalyCause.setPartitionFieldName(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            anomalyCause.setPartitionFieldValue(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            anomalyCause.setFunction(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            anomalyCause.setFunctionDescription(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            anomalyCause.setFieldName(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            anomalyCause.setProbability(randomDouble());
        }
        if (randomBoolean()) {
            int size = randomInt(10);
            List<Influence> influencers = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                int fieldValuesSize = randomInt(10);
                List<String> fieldValues = new ArrayList<>(fieldValuesSize);
                for (int j = 0; j < fieldValuesSize; j++) {
                    fieldValues.add(randomAlphaOfLengthBetween(1, 20));
                }
                influencers.add(new Influence(randomAlphaOfLengthBetween(1, 20), fieldValues));
            }
            anomalyCause.setInfluencers(influencers);
        }
        return anomalyCause;
    }

    @Override
    protected AnomalyCause doParseInstance(XContentParser parser) {
        return AnomalyCause.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    public void testActualAsGeoPoint() {
        AnomalyCause anomalyCause = new AnomalyCause();

        assertThat(anomalyCause.getActualGeoPoint(), is(nullValue()));

        anomalyCause.setFunction(DetectorFunction.LAT_LONG.getFullName());
        assertThat(anomalyCause.getActualGeoPoint(), is(nullValue()));

        anomalyCause.setActual(Collections.singletonList(80.0));
        assertThat(anomalyCause.getActualGeoPoint(), is(nullValue()));

        anomalyCause.setActual(Arrays.asList(90.0, 80.0));
        assertThat(anomalyCause.getActualGeoPoint(), equalTo(new GeoPoint(90.0, 80.0)));

        anomalyCause.setActual(Arrays.asList(10.0, 100.0, 90.0));
        assertThat(anomalyCause.getActualGeoPoint(), is(nullValue()));
    }

    public void testTypicalAsGeoPoint() {
        AnomalyCause anomalyCause = new AnomalyCause();

        assertThat(anomalyCause.getTypicalGeoPoint(), is(nullValue()));

        anomalyCause.setFunction(DetectorFunction.LAT_LONG.getFullName());
        assertThat(anomalyCause.getTypicalGeoPoint(), is(nullValue()));

        anomalyCause.setTypical(Collections.singletonList(80.0));
        assertThat(anomalyCause.getTypicalGeoPoint(), is(nullValue()));

        anomalyCause.setTypical(Arrays.asList(90.0, 80.0));
        assertThat(anomalyCause.getTypicalGeoPoint(), equalTo(new GeoPoint(90.0, 80.0)));

        anomalyCause.setTypical(Arrays.asList(10.0, 100.0, 90.0));
        assertThat(anomalyCause.getTypicalGeoPoint(), is(nullValue()));
    }
}
