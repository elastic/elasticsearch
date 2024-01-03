/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

public class DenseVectorStatsTests extends AbstractWireSerializingTestCase<DenseVectorStats> {
    @Override
    protected Writeable.Reader<DenseVectorStats> instanceReader() {
        return DenseVectorStats::new;
    }

    @Override
    protected DenseVectorStats createTestInstance() {
        DenseVectorStats stats = new DenseVectorStats(randomNonNegativeLong());
        return stats;
    }

    @Override
    protected DenseVectorStats mutateInstance(DenseVectorStats instance) {
        return new DenseVectorStats(randomValueOtherThan(instance.getValueCount(), ESTestCase::randomNonNegativeLong));
    }
}
