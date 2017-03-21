/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.state;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.support.AbstractSerializingTestCase;

import java.util.Date;

public class ModelSnapshotTests extends AbstractSerializingTestCase<ModelSnapshot> {
    private static final Date DEFAULT_TIMESTAMP = new Date();
    private static final String DEFAULT_DESCRIPTION = "a snapshot";
    private static final String DEFAULT_ID = "my_id";
    private static final int DEFAULT_DOC_COUNT = 7;
    private static final Date DEFAULT_LATEST_RESULT_TIMESTAMP = new Date(12345678901234L);
    private static final Date DEFAULT_LATEST_RECORD_TIMESTAMP = new Date(12345678904321L);

    public void testCopyBuilder() {
        ModelSnapshot modelSnapshot1 = createFullyPopulated().build();
        ModelSnapshot modelSnapshot2 = new ModelSnapshot.Builder(modelSnapshot1).build();
        assertEquals(modelSnapshot1, modelSnapshot2);
    }

    public void testEquals_GivenSameObject() {
        ModelSnapshot modelSnapshot = createFullyPopulated().build();
        assertTrue(modelSnapshot.equals(modelSnapshot));
    }

    public void testEquals_GivenObjectOfDifferentClass() {
        ModelSnapshot modelSnapshot = createFullyPopulated().build();
        assertFalse(modelSnapshot.equals("a string"));
    }

    public void testEquals_GivenEqualModelSnapshots() {
        ModelSnapshot modelSnapshot1 = createFullyPopulated().build();
        ModelSnapshot modelSnapshot2 = createFullyPopulated().build();

        assertEquals(modelSnapshot1, modelSnapshot2);
        assertEquals(modelSnapshot2, modelSnapshot1);
        assertEquals(modelSnapshot1.hashCode(), modelSnapshot2.hashCode());
    }

    public void testEquals_GivenDifferentTimestamp() {
        ModelSnapshot modelSnapshot1 = createFullyPopulated().build();
        ModelSnapshot modelSnapshot2 = createFullyPopulated().setTimestamp(
                new Date(modelSnapshot1.getTimestamp().getTime() + 1)).build();

        assertFalse(modelSnapshot1.equals(modelSnapshot2));
        assertFalse(modelSnapshot2.equals(modelSnapshot1));
    }

    public void testEquals_GivenDifferentDescription() {
        ModelSnapshot modelSnapshot1 = createFullyPopulated().build();
        ModelSnapshot modelSnapshot2 = createFullyPopulated()
                .setDescription(modelSnapshot1.getDescription() + " blah").build();

        assertFalse(modelSnapshot1.equals(modelSnapshot2));
        assertFalse(modelSnapshot2.equals(modelSnapshot1));
    }

    public void testEquals_GivenDifferentId() {
        ModelSnapshot modelSnapshot1 = createFullyPopulated().build();
        ModelSnapshot modelSnapshot2 = createFullyPopulated()
                .setSnapshotId(modelSnapshot1.getSnapshotId() + "_2").build();

        assertFalse(modelSnapshot1.equals(modelSnapshot2));
        assertFalse(modelSnapshot2.equals(modelSnapshot1));
    }

    public void testEquals_GivenDifferentDocCount() {
        ModelSnapshot modelSnapshot1 = createFullyPopulated().build();
        ModelSnapshot modelSnapshot2 = createFullyPopulated()
                .setSnapshotDocCount(modelSnapshot1.getSnapshotDocCount() + 1).build();

        assertFalse(modelSnapshot1.equals(modelSnapshot2));
        assertFalse(modelSnapshot2.equals(modelSnapshot1));
    }

    public void testEquals_GivenDifferentModelSizeStats() {
        ModelSnapshot modelSnapshot1 = createFullyPopulated().build();
        ModelSizeStats.Builder modelSizeStats = new ModelSizeStats.Builder("foo");
        modelSizeStats.setModelBytes(42L);
        ModelSnapshot modelSnapshot2 = createFullyPopulated().setModelSizeStats(modelSizeStats).build();

        assertFalse(modelSnapshot1.equals(modelSnapshot2));
        assertFalse(modelSnapshot2.equals(modelSnapshot1));
    }

    public void testEquals_GivenDifferentQuantiles() {
        ModelSnapshot modelSnapshot1 = createFullyPopulated().build();
        ModelSnapshot modelSnapshot2 = createFullyPopulated()
                .setQuantiles(new Quantiles("foo", modelSnapshot1.getQuantiles().getTimestamp(),
                        "different state")).build();

        assertFalse(modelSnapshot1.equals(modelSnapshot2));
        assertFalse(modelSnapshot2.equals(modelSnapshot1));
    }

    public void testEquals_GivenDifferentLatestResultTimestamp() {
        ModelSnapshot modelSnapshot1 = createFullyPopulated().build();
        ModelSnapshot modelSnapshot2 = createFullyPopulated().setLatestResultTimeStamp(
                new Date(modelSnapshot1.getLatestResultTimeStamp().getTime() + 1)).build();

        assertFalse(modelSnapshot1.equals(modelSnapshot2));
        assertFalse(modelSnapshot2.equals(modelSnapshot1));
    }

    public void testEquals_GivenDifferentLatestRecordTimestamp() {
        ModelSnapshot modelSnapshot1 = createFullyPopulated().build();
        ModelSnapshot modelSnapshot2 = createFullyPopulated().setLatestRecordTimeStamp(
                new Date(modelSnapshot1.getLatestRecordTimeStamp().getTime() + 1)).build();

        assertFalse(modelSnapshot1.equals(modelSnapshot2));
        assertFalse(modelSnapshot2.equals(modelSnapshot1));
    }

    private static ModelSnapshot.Builder createFullyPopulated() {
        ModelSnapshot.Builder modelSnapshot = new ModelSnapshot.Builder();
        modelSnapshot.setJobId("foo");
        modelSnapshot.setTimestamp(DEFAULT_TIMESTAMP);
        modelSnapshot.setDescription(DEFAULT_DESCRIPTION);
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
        return createRandomized();
    }

    public static ModelSnapshot createRandomized() {
        ModelSnapshot.Builder modelSnapshot = new ModelSnapshot.Builder(randomAsciiOfLengthBetween(1, 20));
        modelSnapshot.setTimestamp(new Date(TimeValue.parseTimeValue(randomTimeValue(), "test").millis()));
        modelSnapshot.setDescription(randomAsciiOfLengthBetween(1, 20));
        modelSnapshot.setSnapshotId(randomAsciiOfLengthBetween(1, 20));
        modelSnapshot.setSnapshotDocCount(randomInt());
        modelSnapshot.setModelSizeStats(ModelSizeStatsTests.createRandomized());
        modelSnapshot.setLatestResultTimeStamp(
                new Date(TimeValue.parseTimeValue(randomTimeValue(), "test").millis()));
        modelSnapshot.setLatestRecordTimeStamp(
                new Date(TimeValue.parseTimeValue(randomTimeValue(), "test").millis()));
        modelSnapshot.setQuantiles(QuantilesTests.createRandomized());
        return modelSnapshot.build();
    }

    @Override
    protected Reader<ModelSnapshot> instanceReader() {
        return ModelSnapshot::new;
    }

    @Override
    protected ModelSnapshot parseInstance(XContentParser parser) {
        return ModelSnapshot.PARSER.apply(parser, null).build();
    }

    public void testDocumentId() {
        ModelSnapshot snapshot1 = new ModelSnapshot.Builder("foo").setSnapshotId("1").build();
        ModelSnapshot snapshot2 = new ModelSnapshot.Builder("foo").setSnapshotId("2").build();
        ModelSnapshot snapshot3 = new ModelSnapshot.Builder("bar").setSnapshotId("1").build();

        assertEquals("foo-1", ModelSnapshot.documentId(snapshot1));
        assertEquals("foo-2", ModelSnapshot.documentId(snapshot2));
        assertEquals("bar-1", ModelSnapshot.documentId(snapshot3));
    }
}
