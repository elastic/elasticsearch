/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.restore;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.List;

public class RestoreSnapshotResponseTests extends AbstractWireSerializingTestCase<RestoreSnapshotResponse> {

    @Override
    protected RestoreSnapshotResponse createTestInstance() {
        return new RestoreSnapshotResponse(randomBoolean() ? null : mutateRestoreInfo(null));
    }

    private RestoreInfo mutateRestoreInfo(RestoreInfo restoreInfo) {
        if (restoreInfo == null) {
            final var totalShards = between(1, 1000);
            return new RestoreInfo(randomName(), randomIndices(), totalShards, between(0, totalShards));
        }
        if (randomBoolean()) {
            return null;
        }

        return switch (between(1, 4)) {
            case 1 -> new RestoreInfo(
                randomValueOtherThan(restoreInfo.name(), RestoreSnapshotResponseTests::randomName),
                restoreInfo.indices(),
                restoreInfo.totalShards(),
                restoreInfo.successfulShards()
            );
            case 2 -> new RestoreInfo(
                restoreInfo.name(),
                randomValueOtherThan(restoreInfo.indices(), RestoreSnapshotResponseTests::randomIndices),
                restoreInfo.totalShards(),
                restoreInfo.successfulShards()
            );
            case 3 -> new RestoreInfo(
                restoreInfo.name(),
                restoreInfo.indices(),
                randomValueOtherThan(
                    restoreInfo.totalShards(),
                    () -> between(restoreInfo.successfulShards(), Math.max(restoreInfo.totalShards() + 1, 1000))
                ),
                restoreInfo.successfulShards()
            );
            case 4 -> new RestoreInfo(
                restoreInfo.name(),
                restoreInfo.indices(),
                restoreInfo.totalShards(),
                randomValueOtherThan(restoreInfo.successfulShards(), () -> between(0, restoreInfo.totalShards()))
            );
            default -> throw new AssertionError("impossible");
        };
    }

    private static List<String> randomIndices() {
        return randomList(1, 10, () -> randomAlphaOfLength(10));
    }

    private static String randomName() {
        return randomRealisticUnicodeOfCodepointLengthBetween(1, 30);
    }

    @Override
    protected RestoreSnapshotResponse mutateInstance(RestoreSnapshotResponse instance) throws IOException {
        return new RestoreSnapshotResponse(mutateRestoreInfo(instance.getRestoreInfo()));
    }

    @Override
    protected Writeable.Reader<RestoreSnapshotResponse> instanceReader() {
        return RestoreSnapshotResponse::new;
    }
}
