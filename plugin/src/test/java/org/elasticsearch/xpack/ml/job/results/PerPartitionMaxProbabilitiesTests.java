/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.support.AbstractSerializingTestCase;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class PerPartitionMaxProbabilitiesTests extends AbstractSerializingTestCase<PerPartitionMaxProbabilities> {

    @Override
    protected PerPartitionMaxProbabilities createTestInstance() {
        int num = randomIntBetween(1, 10);
        List<PerPartitionMaxProbabilities.PartitionProbability> pps = new ArrayList<>();
        for (int i=0; i<num; i++) {
            pps.add(new PerPartitionMaxProbabilities.PartitionProbability(randomAsciiOfLength(12), randomDouble()));
        }

        return new PerPartitionMaxProbabilities(randomAsciiOfLength(20), new DateTime(randomDateTimeZone()).toDate(),
                randomNonNegativeLong(), pps);
    }

    @Override
    protected Writeable.Reader<PerPartitionMaxProbabilities> instanceReader() {
        return PerPartitionMaxProbabilities::new;
    }

    @Override
    protected PerPartitionMaxProbabilities parseInstance(XContentParser parser) {
        return PerPartitionMaxProbabilities.PARSER.apply(parser, null);
    }

    public void testCreateFromAListOfRecords() {
        List<AnomalyRecord> records = new ArrayList<>();
        records.add(createAnomalyRecord("A", 20.0));
        records.add(createAnomalyRecord("A", 40.0));
        records.add(createAnomalyRecord("B", 90.0));
        records.add(createAnomalyRecord("B", 15.0));
        records.add(createAnomalyRecord("B", 45.0));

        PerPartitionMaxProbabilities ppMax = new PerPartitionMaxProbabilities(records);

        List<PerPartitionMaxProbabilities.PartitionProbability> pProbs = ppMax.getPerPartitionMaxProbabilities();
        assertEquals(2, pProbs.size());
        for (PerPartitionMaxProbabilities.PartitionProbability pProb : pProbs) {
            if (pProb.getPartitionValue().equals("A")) {
                assertEquals(40.0, pProb.getMaxNormalizedProbability(), 0.0001);
            } else {
                assertEquals(90.0, pProb.getMaxNormalizedProbability(), 0.0001);
            }
        }
    }

    public void testMaxProbabilityForPartition() {
        List<AnomalyRecord> records = new ArrayList<>();
        records.add(createAnomalyRecord("A", 20.0));
        records.add(createAnomalyRecord("A", 40.0));
        records.add(createAnomalyRecord("B", 90.0));
        records.add(createAnomalyRecord("B", 15.0));
        records.add(createAnomalyRecord("B", 45.0));

        PerPartitionMaxProbabilities ppMax = new PerPartitionMaxProbabilities(records);

        assertEquals(40.0, ppMax.getMaxProbabilityForPartition("A"), 0.0001);
        assertEquals(90.0, ppMax.getMaxProbabilityForPartition("B"), 0.0001);
    }

    private AnomalyRecord createAnomalyRecord(String partitionFieldValue, double normalizedProbability) {
        AnomalyRecord record = new AnomalyRecord("foo", new Date(), 600, 1);
        record.setPartitionFieldValue(partitionFieldValue);
        record.setNormalizedProbability(normalizedProbability);
        return record;
    }
}
