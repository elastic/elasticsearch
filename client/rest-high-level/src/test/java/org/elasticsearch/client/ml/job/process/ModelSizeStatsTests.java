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

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.util.Date;

import static org.elasticsearch.client.ml.job.process.ModelSizeStats.CategorizationStatus;
import static org.elasticsearch.client.ml.job.process.ModelSizeStats.MemoryStatus;

public class ModelSizeStatsTests extends AbstractXContentTestCase<ModelSizeStats> {

    public void testDefaultConstructor() {
        ModelSizeStats stats = new ModelSizeStats.Builder("foo").build();
        assertEquals(0, stats.getModelBytes());
        assertNull(stats.getModelBytesExceeded());
        assertNull(stats.getModelBytesMemoryLimit());
        assertEquals(0, stats.getTotalByFieldCount());
        assertEquals(0, stats.getTotalOverFieldCount());
        assertEquals(0, stats.getTotalPartitionFieldCount());
        assertEquals(0, stats.getBucketAllocationFailuresCount());
        assertEquals(MemoryStatus.OK, stats.getMemoryStatus());
        assertEquals(0, stats.getCategorizedDocCount());
        assertEquals(0, stats.getTotalCategoryCount());
        assertEquals(0, stats.getFrequentCategoryCount());
        assertEquals(0, stats.getRareCategoryCount());
        assertEquals(0, stats.getDeadCategoryCount());
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
