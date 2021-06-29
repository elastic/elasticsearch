/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.process.autodetect.state;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ModelSnapshotTests extends AbstractSerializingTestCase<ModelSnapshot> {
    private static final Date DEFAULT_TIMESTAMP = new Date();
    private static final String DEFAULT_DESCRIPTION = "a snapshot";
    private static final String DEFAULT_ID = "my_id";
    private static final int DEFAULT_DOC_COUNT = 7;
    private static final Date DEFAULT_LATEST_RESULT_TIMESTAMP = new Date(12345678901234L);
    private static final Date DEFAULT_LATEST_RECORD_TIMESTAMP = new Date(12345678904321L);
    private static final boolean DEFAULT_RETAIN = true;

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
        modelSnapshot.setMinVersion(Version.CURRENT);
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
        modelSnapshot.setRetain(DEFAULT_RETAIN);
        return modelSnapshot;
    }

    @Override
    protected ModelSnapshot createTestInstance() {
        return createRandomized();
    }

    public static ModelSnapshot createRandomized() {
        ModelSnapshot.Builder modelSnapshot = new ModelSnapshot.Builder(randomAlphaOfLengthBetween(1, 20));
        modelSnapshot.setMinVersion(Version.CURRENT);
        modelSnapshot.setTimestamp(new Date(TimeValue.parseTimeValue(randomTimeValue(), "test").millis()));
        modelSnapshot.setDescription(randomAlphaOfLengthBetween(1, 20));
        modelSnapshot.setSnapshotId(randomAlphaOfLength(10));
        modelSnapshot.setSnapshotDocCount(randomInt());
        modelSnapshot.setModelSizeStats(ModelSizeStatsTests.createRandomized());
        modelSnapshot.setLatestResultTimeStamp(
                new Date(TimeValue.parseTimeValue(randomTimeValue(), "test").millis()));
        modelSnapshot.setLatestRecordTimeStamp(
                new Date(TimeValue.parseTimeValue(randomTimeValue(), "test").millis()));
        modelSnapshot.setQuantiles(QuantilesTests.createRandomized());
        modelSnapshot.setRetain(randomBoolean());
        return modelSnapshot.build();
    }

    @Override
    protected Reader<ModelSnapshot> instanceReader() {
        return ModelSnapshot::new;
    }

    @Override
    protected ModelSnapshot doParseInstance(XContentParser parser) {
        return ModelSnapshot.STRICT_PARSER.apply(parser, null).build();
    }

    public void testDocumentId() {
        ModelSnapshot snapshot1 = new ModelSnapshot.Builder("foo").setSnapshotId("1").build();
        ModelSnapshot snapshot2 = new ModelSnapshot.Builder("foo").setSnapshotId("2").build();
        ModelSnapshot snapshot3 = new ModelSnapshot.Builder("bar").setSnapshotId("1").build();

        assertEquals("foo_model_snapshot_1", ModelSnapshot.documentId(snapshot1));
        assertEquals("foo_model_snapshot_2", ModelSnapshot.documentId(snapshot2));
        assertEquals("bar_model_snapshot_1", ModelSnapshot.documentId(snapshot3));
    }

    public void testStateDocumentIds_GivenDocCountIsOne() {
        ModelSnapshot snapshot = new ModelSnapshot.Builder("foo").setSnapshotId("1").setSnapshotDocCount(1).build();
        assertThat(snapshot.stateDocumentIds(), equalTo(Arrays.asList("foo_model_state_1#1")));
    }

    public void testStateDocumentIds_GivenDocCountIsThree() {
        ModelSnapshot snapshot = new ModelSnapshot.Builder("foo").setSnapshotId("123456789").setSnapshotDocCount(3).build();
        assertThat(snapshot.stateDocumentIds(),
                equalTo(Arrays.asList("foo_model_state_123456789#1", "foo_model_state_123456789#2", "foo_model_state_123456789#3")));
    }

    public void testStrictParser() throws IOException {
        String json = "{\"foo\":\"bar\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                    () -> ModelSnapshot.STRICT_PARSER.apply(parser, null));

            assertThat(e.getMessage(), containsString("unknown field [foo]"));
        }
    }

    public void testLenientParser() throws IOException {
        String json = "{\"foo\":\"bar\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            ModelSnapshot.LENIENT_PARSER.apply(parser, null);
        }
    }

    public void testEmptySnapshot() {
        ModelSnapshot modelSnapshot = ModelSnapshot.emptySnapshot("my_job");
        assertThat(modelSnapshot.getSnapshotId(), equalTo("empty"));
        assertThat(modelSnapshot.isTheEmptySnapshot(), is(true));
        assertThat(modelSnapshot.getMinVersion(), equalTo(Version.CURRENT));
        assertThat(modelSnapshot.getLatestRecordTimeStamp(), is(nullValue()));
        assertThat(modelSnapshot.getLatestResultTimeStamp(), is(nullValue()));
    }

    public void testIsEmpty_GivenNonEmptySnapshot() {
        ModelSnapshot modelSnapshot = createRandomized();
        assertThat(modelSnapshot.isTheEmptySnapshot(), is(false));
    }
}
