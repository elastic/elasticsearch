/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml.job.process;

import org.elasticsearch.Version;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.util.Date;

public class ModelSnapshotTests extends AbstractXContentTestCase<ModelSnapshot> {

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
        return createRandomizedBuilder().build();
    }

    public static ModelSnapshot.Builder createRandomizedBuilder() {
        ModelSnapshot.Builder modelSnapshot = new ModelSnapshot.Builder(randomAlphaOfLengthBetween(1, 20));
        modelSnapshot.setMinVersion(Version.CURRENT);
        modelSnapshot.setTimestamp(new Date(TimeValue.parseTimeValue(randomTimeValue(), "test").millis()));
        modelSnapshot.setDescription(randomAlphaOfLengthBetween(1, 20));
        modelSnapshot.setSnapshotId(randomAlphaOfLengthBetween(1, 20));
        modelSnapshot.setSnapshotDocCount(randomInt());
        modelSnapshot.setModelSizeStats(ModelSizeStatsTests.createRandomized());
        modelSnapshot.setLatestResultTimeStamp(
                new Date(TimeValue.parseTimeValue(randomTimeValue(), "test").millis()));
        modelSnapshot.setLatestRecordTimeStamp(
                new Date(TimeValue.parseTimeValue(randomTimeValue(), "test").millis()));
        modelSnapshot.setQuantiles(QuantilesTests.createRandomized());
        modelSnapshot.setRetain(randomBoolean());
        return modelSnapshot;
    }

    @Override
    protected ModelSnapshot doParseInstance(XContentParser parser){
        return ModelSnapshot.PARSER.apply(parser, null).build();
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
