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

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.index.shard.IndexLongFieldRangeTestUtils.checkForSameInstances;
import static org.elasticsearch.index.shard.IndexLongFieldRangeTestUtils.randomRange;

public class IndexLongFieldRangeWireTests extends AbstractWireSerializingTestCase<IndexLongFieldRange> {
    @Override
    protected Writeable.Reader<IndexLongFieldRange> instanceReader() {
        return IndexLongFieldRange::readFrom;
    }

    @Override
    protected IndexLongFieldRange createTestInstance() {
        return randomRange();
    }

    @Override
    protected IndexLongFieldRange mutateInstance(IndexLongFieldRange instance) throws IOException {
        if (instance == IndexLongFieldRange.UNKNOWN) {
            return IndexLongFieldRangeTestUtils.randomSpecificRange();
        }

        if (randomBoolean()) {
            return IndexLongFieldRange.UNKNOWN;
        }

        while (true) {
            final IndexLongFieldRange newInstance = IndexLongFieldRangeTestUtils.randomSpecificRange();
            if (newInstance.getMinUnsafe() != instance.getMinUnsafe()
                || newInstance.getMaxUnsafe() != instance.getMaxUnsafe()
                || Arrays.equals(newInstance.getShards(), instance.getShards()) == false) {
                return newInstance;
            }
        }
    }

    @Override
    protected void assertEqualInstances(IndexLongFieldRange expectedInstance, IndexLongFieldRange newInstance) {
        if (checkForSameInstances(expectedInstance, newInstance) == false) {
            super.assertEqualInstances(expectedInstance, newInstance);
        }
    }

}
