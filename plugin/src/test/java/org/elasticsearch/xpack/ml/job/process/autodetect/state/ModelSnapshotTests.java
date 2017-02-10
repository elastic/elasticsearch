/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.state;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSizeStats.MemoryStatus;
import org.elasticsearch.xpack.ml.support.AbstractSerializingTestCase;
import org.elasticsearch.xpack.ml.utils.time.TimeUtils;

import java.util.Date;

public class ModelSnapshotTests extends AbstractSerializingTestCase<ModelSnapshot> {
    private static final Date DEFAULT_TIMESTAMP = new Date();
    private static final String DEFAULT_DESCRIPTION = "a snapshot";
    private static final String DEFAULT_ID = "my_id";
    private static final long DEFAULT_PRIORITY = 1234L;
    private static final int DEFAULT_DOC_COUNT = 7;
    private static final Date DEFAULT_LATEST_RESULT_TIMESTAMP = new Date(12345678901234L);
    private static final Date DEFAULT_LATEST_RECORD_TIMESTAMP = new Date(12345678904321L);


    public void testEquals_GivenSameObject() {
        ModelSnapshot modelSnapshot = new ModelSnapshot(randomAsciiOfLengthBetween(1, 20));

        assertTrue(modelSnapshot.equals(modelSnapshot));
    }


    public void testEquals_GivenObjectOfDifferentClass() {
        ModelSnapshot modelSnapshot = new ModelSnapshot(randomAsciiOfLengthBetween(1, 20));

        assertFalse(modelSnapshot.equals("a string"));
    }


    public void testEquals_GivenEqualModelSnapshots() {
        ModelSnapshot modelSnapshot1 = createFullyPopulated();
        ModelSnapshot modelSnapshot2 = createFullyPopulated();
        modelSnapshot2.setTimestamp(modelSnapshot1.getTimestamp());

        assertEquals(modelSnapshot1, modelSnapshot2);
        assertEquals(modelSnapshot2, modelSnapshot1);
        assertEquals(modelSnapshot1.hashCode(), modelSnapshot2.hashCode());
    }


    public void testEquals_GivenDifferentTimestamp() {
        ModelSnapshot modelSnapshot1 = createFullyPopulated();
        ModelSnapshot modelSnapshot2 = createFullyPopulated();
        modelSnapshot2.setTimestamp(new Date(modelSnapshot2.getTimestamp().getTime() + 1));

        assertFalse(modelSnapshot1.equals(modelSnapshot2));
        assertFalse(modelSnapshot2.equals(modelSnapshot1));
    }


    public void testEquals_GivenDifferentDescription() {
        ModelSnapshot modelSnapshot1 = createFullyPopulated();
        ModelSnapshot modelSnapshot2 = createFullyPopulated();
        modelSnapshot2.setDescription(modelSnapshot2.getDescription() + " blah");

        assertFalse(modelSnapshot1.equals(modelSnapshot2));
        assertFalse(modelSnapshot2.equals(modelSnapshot1));
    }


    public void testEquals_GivenDifferentRestorePriority() {
        ModelSnapshot modelSnapshot1 = createFullyPopulated();
        ModelSnapshot modelSnapshot2 = createFullyPopulated();
        modelSnapshot2.setRestorePriority(modelSnapshot2.getRestorePriority() + 1);

        assertFalse(modelSnapshot1.equals(modelSnapshot2));
        assertFalse(modelSnapshot2.equals(modelSnapshot1));
    }


    public void testEquals_GivenDifferentId() {
        ModelSnapshot modelSnapshot1 = createFullyPopulated();
        ModelSnapshot modelSnapshot2 = createFullyPopulated();
        modelSnapshot2.setSnapshotId(modelSnapshot2.getSnapshotId() + "_2");

        assertFalse(modelSnapshot1.equals(modelSnapshot2));
        assertFalse(modelSnapshot2.equals(modelSnapshot1));
    }


    public void testEquals_GivenDifferentDocCount() {
        ModelSnapshot modelSnapshot1 = createFullyPopulated();
        ModelSnapshot modelSnapshot2 = createFullyPopulated();
        modelSnapshot2.setSnapshotDocCount(modelSnapshot2.getSnapshotDocCount() + 1);

        assertFalse(modelSnapshot1.equals(modelSnapshot2));
        assertFalse(modelSnapshot2.equals(modelSnapshot1));
    }


    public void testEquals_GivenDifferentModelSizeStats() {
        ModelSnapshot modelSnapshot1 = createFullyPopulated();
        ModelSnapshot modelSnapshot2 = createFullyPopulated();
        ModelSizeStats.Builder modelSizeStats = new ModelSizeStats.Builder("foo");
        modelSizeStats.setModelBytes(42L);
        modelSnapshot2.setModelSizeStats(modelSizeStats);

        assertFalse(modelSnapshot1.equals(modelSnapshot2));
        assertFalse(modelSnapshot2.equals(modelSnapshot1));
    }


    public void testEquals_GivenDifferentQuantiles() {
        ModelSnapshot modelSnapshot1 = createFullyPopulated();
        ModelSnapshot modelSnapshot2 = createFullyPopulated();
        modelSnapshot2.setQuantiles(new Quantiles("foo", modelSnapshot2.getQuantiles().getTimestamp(), "different state"));

        assertFalse(modelSnapshot1.equals(modelSnapshot2));
        assertFalse(modelSnapshot2.equals(modelSnapshot1));
    }


    public void testEquals_GivenDifferentLatestResultTimestamp() {
        ModelSnapshot modelSnapshot1 = createFullyPopulated();
        ModelSnapshot modelSnapshot2 = createFullyPopulated();
        modelSnapshot2.setLatestResultTimeStamp(
                new Date(modelSnapshot2.getLatestResultTimeStamp().getTime() + 1));

        assertFalse(modelSnapshot1.equals(modelSnapshot2));
        assertFalse(modelSnapshot2.equals(modelSnapshot1));
    }


    public void testEquals_GivenDifferentLatestRecordTimestamp() {
        ModelSnapshot modelSnapshot1 = createFullyPopulated();
        ModelSnapshot modelSnapshot2 = createFullyPopulated();
        modelSnapshot2.setLatestRecordTimeStamp(
                new Date(modelSnapshot2.getLatestRecordTimeStamp().getTime() + 1));

        assertFalse(modelSnapshot1.equals(modelSnapshot2));
        assertFalse(modelSnapshot2.equals(modelSnapshot1));
    }


    private static ModelSnapshot createFullyPopulated() {
        ModelSnapshot modelSnapshot = new ModelSnapshot("foo");
        modelSnapshot.setTimestamp(DEFAULT_TIMESTAMP);
        modelSnapshot.setDescription(DEFAULT_DESCRIPTION);
        modelSnapshot.setRestorePriority(DEFAULT_PRIORITY);
        modelSnapshot.setSnapshotId(DEFAULT_ID);
        modelSnapshot.setSnapshotDocCount(DEFAULT_DOC_COUNT);
        ModelSizeStats.Builder modelSizeStatsBuilder = new ModelSizeStats.Builder("foo");
        modelSizeStatsBuilder.setLogTime(null);
        modelSnapshot.setModelSizeStats(modelSizeStatsBuilder);
        modelSnapshot.setLatestResultTimeStamp(DEFAULT_LATEST_RESULT_TIMESTAMP);
        modelSnapshot.setLatestRecordTimeStamp(DEFAULT_LATEST_RECORD_TIMESTAMP);
        modelSnapshot.setQuantiles(new Quantiles("foo", DEFAULT_TIMESTAMP, "state"));
        return modelSnapshot;
    }

    @Override
    protected ModelSnapshot createTestInstance() {
        ModelSnapshot modelSnapshot = new ModelSnapshot(randomAsciiOfLengthBetween(1, 20));
        modelSnapshot.setTimestamp(new Date(TimeUtils.dateStringToEpoch(randomTimeValue())));
        modelSnapshot.setDescription(randomAsciiOfLengthBetween(1, 20));
        modelSnapshot.setRestorePriority(randomLong());
        modelSnapshot.setSnapshotId(randomAsciiOfLengthBetween(1, 20));
        modelSnapshot.setSnapshotDocCount(randomInt());
        ModelSizeStats.Builder stats = new ModelSizeStats.Builder(randomAsciiOfLengthBetween(1, 20));
        if (randomBoolean()) {
            stats.setBucketAllocationFailuresCount(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            stats.setModelBytes(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            stats.setTotalByFieldCount(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            stats.setTotalOverFieldCount(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            stats.setTotalPartitionFieldCount(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            stats.setLogTime(new Date(randomLong()));
        }
        if (randomBoolean()) {
            stats.setTimestamp(new Date(randomLong()));
        }
        if (randomBoolean()) {
            stats.setMemoryStatus(randomFrom(MemoryStatus.values()));
        }
        if (randomBoolean()) {
            stats.setId(randomAsciiOfLengthBetween(1, 20));
        }
        modelSnapshot.setModelSizeStats(stats);
        modelSnapshot.setLatestResultTimeStamp(new Date(TimeUtils.dateStringToEpoch(randomTimeValue())));
        modelSnapshot.setLatestRecordTimeStamp(new Date(TimeUtils.dateStringToEpoch(randomTimeValue())));
        Quantiles quantiles =
                new Quantiles("foo", new Date(TimeUtils.dateStringToEpoch(randomTimeValue())), randomAsciiOfLengthBetween(0, 1000));
        modelSnapshot.setQuantiles(quantiles);
        return modelSnapshot;
    }

    @Override
    protected Reader<ModelSnapshot> instanceReader() {
        return ModelSnapshot::new;
    }

    @Override
    protected ModelSnapshot parseInstance(XContentParser parser) {
        return ModelSnapshot.PARSER.apply(parser, null);
    }
}
