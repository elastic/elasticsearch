/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class IndexFeatureStatsTests extends AbstractWireSerializingTestCase<IndexFeatureStats> {
    @Override
    protected Writeable.Reader<IndexFeatureStats> instanceReader() {
        return IndexFeatureStats::new;
    }

    @Override
    protected IndexFeatureStats createTestInstance() {
        IndexFeatureStats indexFeatureStats = new IndexFeatureStats(randomAlphaOfLengthBetween(3, 10));
        indexFeatureStats.indexCount = randomIntBetween(0, Integer.MAX_VALUE);
        indexFeatureStats.count = randomIntBetween(0, Integer.MAX_VALUE);
        return indexFeatureStats;
    }

    @Override
    protected IndexFeatureStats mutateInstance(IndexFeatureStats instance) {
        String name = instance.name;
        int count = instance.count;
        int indexCount = instance.indexCount;
        switch (between(0, 2)) {
            case 0 -> name = randomValueOtherThan(name, () -> randomAlphaOfLengthBetween(3, 10));
            case 1 -> count = randomValueOtherThan(count, () -> randomIntBetween(0, Integer.MAX_VALUE));
            case 2 -> indexCount = randomValueOtherThan(indexCount, () -> randomIntBetween(0, Integer.MAX_VALUE));
        }
        IndexFeatureStats mutated = new IndexFeatureStats(name);
        mutated.count = count;
        mutated.indexCount = indexCount;
        return mutated;
    }

    public void testToXContent() {
        IndexFeatureStats testInstance = createTestInstance();
        assertEquals(Strings.format("""
            {"name":"%s","count":%s,"index_count":%s}\
            """, testInstance.name, testInstance.count, testInstance.indexCount), Strings.toString(testInstance));
    }
}
