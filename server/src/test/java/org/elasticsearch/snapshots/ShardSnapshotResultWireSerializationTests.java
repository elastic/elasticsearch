/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;

public class ShardSnapshotResultWireSerializationTests extends AbstractWireSerializingTestCase<ShardSnapshotResult> {

    @Override
    protected Writeable.Reader<ShardSnapshotResult> instanceReader() {
        return ShardSnapshotResult::new;
    }

    @Override
    protected ShardSnapshotResult createTestInstance() {
        return randomShardSnapshotResult();
    }

    @Override
    protected ShardSnapshotResult mutateInstance(ShardSnapshotResult instance) throws IOException {
        return mutateShardSnapshotResult(instance);
    }

    public void testToString() {
        final ShardSnapshotResult testInstance = randomShardSnapshotResult();
        assertThat(
            testInstance.toString(),
            allOf(
                containsString(testInstance.getGeneration()),
                containsString(testInstance.getSize().toString()),
                containsString(Integer.toString(testInstance.getSegmentCount()))
            )
        );
    }

    static ShardSnapshotResult randomShardSnapshotResult() {
        return new ShardSnapshotResult(randomAlphaOfLength(5), new ByteSizeValue(randomNonNegativeLong()), between(0, 1000));
    }

    static ShardSnapshotResult mutateShardSnapshotResult(ShardSnapshotResult instance) {
        switch (between(1, 3)) {
            case 1:
                return new ShardSnapshotResult(
                    randomAlphaOfLength(11 - instance.getGeneration().length()),
                    instance.getSize(),
                    instance.getSegmentCount()
                );
            case 2:
                return new ShardSnapshotResult(
                    instance.getGeneration(),
                    randomValueOtherThan(instance.getSize(), () -> new ByteSizeValue(randomNonNegativeLong())),
                    instance.getSegmentCount()
                );

            case 3:
                return new ShardSnapshotResult(
                    instance.getGeneration(),
                    instance.getSize(),
                    randomValueOtherThan(instance.getSegmentCount(), () -> between(0, 1000))
                );
        }
        throw new AssertionError("invalid mutation");
    }

}
