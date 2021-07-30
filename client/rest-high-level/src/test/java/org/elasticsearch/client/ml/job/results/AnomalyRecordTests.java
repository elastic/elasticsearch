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
import java.util.Date;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class AnomalyRecordTests extends AbstractXContentTestCase<AnomalyRecord> {

    @Override
    protected AnomalyRecord createTestInstance() {
        return createTestInstance("foo");
    }

    public static AnomalyRecord createTestInstance(String jobId) {
        AnomalyRecord anomalyRecord = new AnomalyRecord(jobId, new Date(randomNonNegativeLong()), randomNonNegativeLong());
        anomalyRecord.setActual(Collections.singletonList(randomDouble()));
        anomalyRecord.setTypical(Collections.singletonList(randomDouble()));
        anomalyRecord.setProbability(randomDouble());
        if (randomBoolean()) {
            anomalyRecord.setMultiBucketImpact(randomDouble());
        }
        anomalyRecord.setRecordScore(randomDouble());
        anomalyRecord.setInitialRecordScore(randomDouble());
        anomalyRecord.setInterim(randomBoolean());
        if (randomBoolean()) {
            anomalyRecord.setFieldName(randomAlphaOfLength(12));
        }
        if (randomBoolean()) {
            anomalyRecord.setByFieldName(randomAlphaOfLength(12));
            anomalyRecord.setByFieldValue(randomAlphaOfLength(12));
        }
        if (randomBoolean()) {
            anomalyRecord.setPartitionFieldName(randomAlphaOfLength(12));
            anomalyRecord.setPartitionFieldValue(randomAlphaOfLength(12));
        }
        if (randomBoolean()) {
            anomalyRecord.setOverFieldName(randomAlphaOfLength(12));
            anomalyRecord.setOverFieldValue(randomAlphaOfLength(12));
        }
        anomalyRecord.setFunction(randomAlphaOfLengthBetween(5, 20));
        anomalyRecord.setFunctionDescription(randomAlphaOfLengthBetween(5, 20));
        if (randomBoolean()) {
            anomalyRecord.setCorrelatedByFieldValue(randomAlphaOfLength(16));
        }
        if (randomBoolean()) {
            int count = randomIntBetween(0, 9);
            List<Influence>  influences = new ArrayList<>();
            for (int i=0; i<count; i++) {
                influences.add(new Influence(randomAlphaOfLength(8), Collections.singletonList(randomAlphaOfLengthBetween(1, 28))));
            }
            anomalyRecord.setInfluencers(influences);
        }
        if (randomBoolean()) {
            int count = randomIntBetween(0, 9);
            List<AnomalyCause>  causes = new ArrayList<>();
            for (int i=0; i<count; i++) {
                causes.add(new AnomalyCauseTests().createTestInstance());
            }
            anomalyRecord.setCauses(causes);
        }

        return anomalyRecord;
    }

    @Override
    protected AnomalyRecord doParseInstance(XContentParser parser) {
        return AnomalyRecord.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    public void testActualAsGeoPoint() {
        AnomalyRecord anomalyRecord = new AnomalyRecord(randomAlphaOfLength(10), new Date(), randomNonNegativeLong());

        assertThat(anomalyRecord.getActualGeoPoint(), is(nullValue()));

        anomalyRecord.setFunction(DetectorFunction.LAT_LONG.getFullName());
        assertThat(anomalyRecord.getActualGeoPoint(), is(nullValue()));

        anomalyRecord.setActual(Collections.singletonList(80.0));
        assertThat(anomalyRecord.getActualGeoPoint(), is(nullValue()));

        anomalyRecord.setActual(Arrays.asList(90.0, 80.0));
        assertThat(anomalyRecord.getActualGeoPoint(), equalTo(new GeoPoint(90.0, 80.0)));

        anomalyRecord.setActual(Arrays.asList(10.0, 100.0, 90.0));
        assertThat(anomalyRecord.getActualGeoPoint(), is(nullValue()));
    }

    public void testTypicalAsGeoPoint() {
        AnomalyRecord anomalyRecord = new AnomalyRecord(randomAlphaOfLength(10), new Date(), randomNonNegativeLong());

        assertThat(anomalyRecord.getTypicalGeoPoint(), is(nullValue()));

        anomalyRecord.setFunction(DetectorFunction.LAT_LONG.getFullName());
        assertThat(anomalyRecord.getTypicalGeoPoint(), is(nullValue()));

        anomalyRecord.setTypical(Collections.singletonList(80.0));
        assertThat(anomalyRecord.getTypicalGeoPoint(), is(nullValue()));

        anomalyRecord.setTypical(Arrays.asList(90.0, 80.0));
        assertThat(anomalyRecord.getTypicalGeoPoint(), equalTo(new GeoPoint(90.0, 80.0)));

        anomalyRecord.setTypical(Arrays.asList(10.0, 100.0, 90.0));
        assertThat(anomalyRecord.getTypicalGeoPoint(), is(nullValue()));
    }
}
