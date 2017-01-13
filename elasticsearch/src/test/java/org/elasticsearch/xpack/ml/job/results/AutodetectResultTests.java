/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.results;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.job.ModelSizeStats;
import org.elasticsearch.xpack.ml.job.ModelSnapshot;
import org.elasticsearch.xpack.ml.job.process.autodetect.output.FlushAcknowledgement;
import org.elasticsearch.xpack.ml.job.quantiles.Quantiles;
import org.elasticsearch.xpack.ml.support.AbstractSerializingTestCase;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class AutodetectResultTests extends AbstractSerializingTestCase<AutodetectResult> {

    @Override
    protected AutodetectResult parseInstance(XContentParser parser) {
        return AutodetectResult.PARSER.apply(parser, null);
    }

    @Override
    protected AutodetectResult createTestInstance() {
        Bucket bucket;
        List<AnomalyRecord> records = null;
        List<Influencer> influencers = null;
        Quantiles quantiles;
        ModelSnapshot modelSnapshot;
        ModelSizeStats.Builder modelSizeStats;
        ModelDebugOutput modelDebugOutput;
        CategoryDefinition categoryDefinition;
        FlushAcknowledgement flushAcknowledgement;
        String jobId = "foo";
        if (randomBoolean()) {
            bucket = new Bucket(jobId, new Date(randomLong()), randomNonNegativeLong());
        } else {
            bucket = null;
        }
        if (randomBoolean()) {
            int size = randomInt(10);
            records = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                AnomalyRecord record = new AnomalyRecord(jobId, new Date(randomLong()), randomNonNegativeLong(), i + 1);
                record.setProbability(randomDoubleBetween(0.0, 1.0, true));
                records.add(record);
            }

        }
        if (randomBoolean()) {
            int size = randomInt(10);
            influencers = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                Influencer influencer = new Influencer(jobId, randomAsciiOfLength(10), randomAsciiOfLength(10),
                        new Date(randomLong()), randomNonNegativeLong(), i + 1);
                influencer.setProbability(randomDoubleBetween(0.0, 1.0, true));
                influencers.add(influencer);
            }
        }
        if (randomBoolean()) {
            quantiles = new Quantiles(jobId, new Date(randomLong()), randomAsciiOfLengthBetween(1, 20));
        } else {
            quantiles = null;
        }
        if (randomBoolean()) {
            modelSnapshot = new ModelSnapshot(jobId);
            modelSnapshot.setDescription(randomAsciiOfLengthBetween(1, 20));
        } else {
            modelSnapshot = null;
        }
        if (randomBoolean()) {
            modelSizeStats = new ModelSizeStats.Builder(jobId);
            modelSizeStats.setId(randomAsciiOfLengthBetween(1, 20));
        } else {
            modelSizeStats = null;
        }
        if (randomBoolean()) {
            modelDebugOutput = new ModelDebugOutput(jobId);
            modelDebugOutput.setId(randomAsciiOfLengthBetween(1, 20));
        } else {
            modelDebugOutput = null;
        }
        if (randomBoolean()) {
            categoryDefinition = new CategoryDefinition(jobId);
            categoryDefinition.setCategoryId(randomLong());
        } else {
            categoryDefinition = null;
        }
        if (randomBoolean()) {
            flushAcknowledgement = new FlushAcknowledgement(randomAsciiOfLengthBetween(1, 20));
        } else {
            flushAcknowledgement = null;
        }
        return new AutodetectResult(bucket, records, influencers, quantiles, modelSnapshot,
                modelSizeStats == null ? null : modelSizeStats.build(), modelDebugOutput, categoryDefinition, flushAcknowledgement);
    }

    @Override
    protected Reader<AutodetectResult> instanceReader() {
        return AutodetectResult::new;
    }

}
