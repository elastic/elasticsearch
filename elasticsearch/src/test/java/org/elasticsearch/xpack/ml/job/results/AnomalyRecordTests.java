/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.support.AbstractSerializingTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class AnomalyRecordTests extends AbstractSerializingTestCase<AnomalyRecord> {

    @Override
    protected AnomalyRecord createTestInstance() {
        return createTestInstance("foo", 1);
    }

    public AnomalyRecord createTestInstance(String jobId, int sequenceNum) {
        AnomalyRecord anomalyRecord = new AnomalyRecord(jobId, new Date(randomNonNegativeLong()), randomNonNegativeLong(), sequenceNum);
        anomalyRecord.setActual(Collections.singletonList(randomDouble()));
        anomalyRecord.setTypical(Collections.singletonList(randomDouble()));
        anomalyRecord.setAnomalyScore(randomDouble());
        anomalyRecord.setProbability(randomDouble());
        anomalyRecord.setNormalizedProbability(randomDouble());
        anomalyRecord.setInitialNormalizedProbability(randomDouble());
        anomalyRecord.setInterim(randomBoolean());
        if (randomBoolean()) {
            anomalyRecord.setFieldName(randomAsciiOfLength(12));
        }
        if (randomBoolean()) {
            anomalyRecord.setByFieldName(randomAsciiOfLength(12));
            anomalyRecord.setByFieldValue(randomAsciiOfLength(12));
        }
        if (randomBoolean()) {
            anomalyRecord.setPartitionFieldName(randomAsciiOfLength(12));
            anomalyRecord.setPartitionFieldValue(randomAsciiOfLength(12));
        }
        if (randomBoolean()) {
            anomalyRecord.setOverFieldName(randomAsciiOfLength(12));
            anomalyRecord.setOverFieldValue(randomAsciiOfLength(12));
        }
        anomalyRecord.setFunction(randomAsciiOfLengthBetween(5, 20));
        anomalyRecord.setFunctionDescription(randomAsciiOfLengthBetween(5, 20));
        if (randomBoolean()) {
            anomalyRecord.setCorrelatedByFieldValue(randomAsciiOfLength(16));
        }
        if (randomBoolean()) {
            int count = randomIntBetween(0, 9);
            List<Influence>  influences = new ArrayList<>();
            for (int i=0; i<count; i++) {
                influences.add(new Influence(randomAsciiOfLength(8), Collections.singletonList(randomAsciiOfLengthBetween(1, 28))));
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
    protected Writeable.Reader<AnomalyRecord> instanceReader() {
        return AnomalyRecord::new;
    }

    @Override
    protected AnomalyRecord parseInstance(XContentParser parser) {
        return AnomalyRecord.PARSER.apply(parser, null);
    }
}
