/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

public class SparseVectorStatsTests extends AbstractWireSerializingTestCase<SparseVectorStats> {
    @Override
    protected Writeable.Reader<SparseVectorStats> instanceReader() {
        return SparseVectorStats::new;
    }

    @Override
    protected SparseVectorStats createTestInstance() {
        return new SparseVectorStats(randomNonNegativeLong());
    }

    @Override
    protected SparseVectorStats mutateInstance(SparseVectorStats instance) {
        return new SparseVectorStats(randomValueOtherThan(instance.getValueCount(), ESTestCase::randomNonNegativeLong));
    }
}
