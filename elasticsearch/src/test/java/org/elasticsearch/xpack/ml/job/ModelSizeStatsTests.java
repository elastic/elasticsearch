/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.job.ModelSizeStats.MemoryStatus;
import org.elasticsearch.xpack.ml.support.AbstractSerializingTestCase;

import java.util.Date;

public class ModelSizeStatsTests extends AbstractSerializingTestCase<ModelSizeStats> {

    public void testDefaultConstructor() {
        ModelSizeStats stats = new ModelSizeStats.Builder("foo").build();
        assertEquals(0, stats.getModelBytes());
        assertEquals(0, stats.getTotalByFieldCount());
        assertEquals(0, stats.getTotalOverFieldCount());
        assertEquals(0, stats.getTotalPartitionFieldCount());
        assertEquals(0, stats.getBucketAllocationFailuresCount());
        assertEquals(MemoryStatus.OK, stats.getMemoryStatus());
    }

    public void testDocumentId() {
        assertEquals("foo-model_size_stats", ModelSizeStats.documentId("foo"));
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
        ModelSizeStats.Builder stats = new ModelSizeStats.Builder("foo");
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
        return stats.build();
    }

    @Override
    protected Reader<ModelSizeStats> instanceReader() {
        return ModelSizeStats::new;
    }

    @Override
    protected ModelSizeStats parseInstance(XContentParser parser) {
        return ModelSizeStats.PARSER.apply(parser, null).build();
    }
}
