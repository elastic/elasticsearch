/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.job.process;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.util.Date;

import static org.elasticsearch.client.ml.job.process.ModelSizeStats.AssignmentMemoryBasis;
import static org.elasticsearch.client.ml.job.process.ModelSizeStats.CategorizationStatus;
import static org.elasticsearch.client.ml.job.process.ModelSizeStats.MemoryStatus;

public class ModelSizeStatsTests extends AbstractXContentTestCase<ModelSizeStats> {

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
            stats.setMemoryStatus(randomFrom(MemoryStatus.values()));
        }
        if (randomBoolean()) {
            stats.setAssignmentMemoryBasis(randomFrom(AssignmentMemoryBasis.values()));
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
        if (randomBoolean()) {
            stats.setLogTime(new Date(TimeValue.parseTimeValue(randomTimeValue(), "test").millis()));
        }
        if (randomBoolean()) {
            stats.setTimestamp(new Date(TimeValue.parseTimeValue(randomTimeValue(), "test").millis()));
        }
        return stats.build();
    }

    @Override
    protected ModelSizeStats doParseInstance(XContentParser parser) {
        return ModelSizeStats.PARSER.apply(parser, null).build();
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
