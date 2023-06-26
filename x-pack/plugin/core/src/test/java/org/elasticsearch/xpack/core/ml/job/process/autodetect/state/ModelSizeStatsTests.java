/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.process.autodetect.state;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats.MemoryStatus;

import java.io.IOException;
import java.util.Date;

import static org.hamcrest.Matchers.containsString;

public class ModelSizeStatsTests extends AbstractXContentSerializingTestCase<ModelSizeStats> {

    public void testDefaultConstructor() {
        ModelSizeStats stats = new ModelSizeStats.Builder("foo").build();
        assertEquals(0, stats.getModelBytes());
        assertNull(stats.getPeakModelBytes());
        assertNull(stats.getModelBytesExceeded());
        assertNull(stats.getModelBytesMemoryLimit());
        assertEquals(0, stats.getTotalByFieldCount());
        assertEquals(0, stats.getTotalOverFieldCount());
        assertEquals(0, stats.getTotalPartitionFieldCount());
        assertEquals(0, stats.getBucketAllocationFailuresCount());
        assertEquals(MemoryStatus.OK, stats.getMemoryStatus());
        assertNull(stats.getAssignmentMemoryBasis());
        assertEquals(0, stats.getCategorizedDocCount());
        assertEquals(0, stats.getTotalCategoryCount());
        assertEquals(0, stats.getFrequentCategoryCount());
        assertEquals(0, stats.getRareCategoryCount());
        assertEquals(0, stats.getDeadCategoryCount());
        assertEquals(0, stats.getFailedCategoryCount());
        assertEquals(CategorizationStatus.OK, stats.getCategorizationStatus());
    }

    public void testSetMemoryStatus_GivenNull() {
        ModelSizeStats.Builder stats = new ModelSizeStats.Builder("foo");

        NullPointerException ex = expectThrows(NullPointerException.class, () -> stats.setMemoryStatus(null));

        assertEquals("[memory_status] must not be null", ex.getMessage());
    }

    public void testSetMemoryStatus_GivenSoftLimit() {
        ModelSizeStats.Builder stats = new ModelSizeStats.Builder("foo");

        stats.setMemoryStatus(MemoryStatus.SOFT_LIMIT);

        assertEquals(MemoryStatus.SOFT_LIMIT, stats.build().getMemoryStatus());
    }

    @Override
    protected ModelSizeStats createTestInstance() {
        return createRandomized();
    }

    @Override
    protected ModelSizeStats mutateInstance(ModelSizeStats instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    public static ModelSizeStats createRandomized() {
        ModelSizeStats.Builder stats = new ModelSizeStats.Builder("foo");
        if (randomBoolean()) {
            stats.setBucketAllocationFailuresCount(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            stats.setModelBytes(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            stats.setPeakModelBytes(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            stats.setModelBytesExceeded(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            stats.setModelBytesMemoryLimit(randomNonNegativeLong());
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
            stats.setLogTime(new Date(TimeValue.parseTimeValue(randomTimeValue(), "test").millis()));
        }
        if (randomBoolean()) {
            stats.setTimestamp(new Date(TimeValue.parseTimeValue(randomTimeValue(), "test").millis()));
        }
        if (randomBoolean()) {
            stats.setMemoryStatus(randomFrom(MemoryStatus.values()));
        }
        if (randomBoolean()) {
            stats.setAssignmentMemoryBasis(randomFrom(ModelSizeStats.AssignmentMemoryBasis.values()));
        }
        if (randomBoolean()) {
            stats.setCategorizedDocCount(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            stats.setTotalCategoryCount(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            stats.setFrequentCategoryCount(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            stats.setRareCategoryCount(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            stats.setDeadCategoryCount(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            stats.setFailedCategoryCount(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            stats.setCategorizationStatus(randomFrom(CategorizationStatus.values()));
        }
        return stats.build();
    }

    @Override
    protected Reader<ModelSizeStats> instanceReader() {
        return ModelSizeStats::new;
    }

    @Override
    protected ModelSizeStats doParseInstance(XContentParser parser) {
        return ModelSizeStats.STRICT_PARSER.apply(parser, null).build();
    }

    public void testId() {
        ModelSizeStats stats = new ModelSizeStats.Builder("job-foo").setLogTime(new Date(100)).build();
        assertEquals("job-foo_model_size_stats_100", stats.getId());
    }

    public void testStrictParser() throws IOException {
        String json = "{\"job_id\":\"job_1\", \"foo\":\"bar\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> ModelSizeStats.STRICT_PARSER.apply(parser, null)
            );

            assertThat(e.getMessage(), containsString("unknown field [foo]"));
        }
    }

    public void testLenientParser() throws IOException {
        String json = "{\"job_id\":\"job_1\", \"foo\":\"bar\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            ModelSizeStats.LENIENT_PARSER.apply(parser, null);
        }
    }
}
