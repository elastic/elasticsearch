/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.job.process.autodetect.state;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

public class CategorizerStatsTests extends AbstractSerializingTestCase<CategorizerStats> {

    public void testDefaultConstructor() {
        CategorizerStats stats = new CategorizerStats.Builder("foo").build();
        assertNull(stats.getPartitionFieldName());
        assertNull(stats.getPartitionFieldValue());
        assertEquals(0, stats.getCategorizedDocCount());
        assertEquals(0, stats.getTotalCategoryCount());
        assertEquals(0, stats.getFrequentCategoryCount());
        assertEquals(0, stats.getRareCategoryCount());
        assertEquals(0, stats.getDeadCategoryCount());
        assertEquals(0, stats.getFailedCategoryCount());
        assertEquals(CategorizationStatus.OK, stats.getCategorizationStatus());
    }

    @Override
    protected CategorizerStats createTestInstance() {
        return createRandomized("foo");
    }

    public static CategorizerStats createRandomized(String jobId) {
        CategorizerStats.Builder stats = new CategorizerStats.Builder(jobId);
        if (randomBoolean()) {
            stats.setPartitionFieldName(randomAlphaOfLength(10));
            stats.setPartitionFieldValue(randomAlphaOfLength(20));
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
    protected Writeable.Reader<CategorizerStats> instanceReader() {
        return CategorizerStats::new;
    }

    @Override
    protected CategorizerStats doParseInstance(XContentParser parser) {
        // Lenient because the partitionFieldName/Value pair is added as a separate field
        return CategorizerStats.LENIENT_PARSER.apply(parser, null).build();
    }

    @Override
    protected boolean supportsUnknownFields() {
        // Because the partitionFieldName/Value pair is added as a separate field
        return true;
    }
}
