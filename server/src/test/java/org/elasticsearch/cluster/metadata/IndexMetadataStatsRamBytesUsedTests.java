/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.test.AbstractAccountableFieldsTestCase;

import java.util.Set;

import static org.hamcrest.Matchers.greaterThan;

public class IndexMetadataStatsRamBytesUsedTests extends AbstractAccountableFieldsTestCase {

    @Override
    protected Class<? extends Accountable> classUnderTest() {
        return IndexMetadataStats.class;
    }

    @Override
    protected Set<String> fieldsAccountedForInRamBytesUsed() {
        return Set.of("indexWriteLoad", "averageShardSize");
    }

    @Override
    protected Accountable createRandomTestInstance() {
        int shards = randomIntBetween(1, 32);
        return new IndexMetadataStats(IndexWriteLoad.builder(shards).build(), randomNonNegativeLong(), shards);
    }

    /**
     * Non-tautology check: the stats estimate must include the nested {@link IndexWriteLoad}, so more shards means a larger estimate.
     */
    public void testRamBytesUsedIncludesWriteLoad() {
        IndexMetadataStats few = new IndexMetadataStats(IndexWriteLoad.builder(1).build(), 100L, 1);
        IndexMetadataStats many = new IndexMetadataStats(IndexWriteLoad.builder(16).build(), 100L, 16);
        assertThat(many.ramBytesUsed(), greaterThan(few.ramBytesUsed()));
    }
}
