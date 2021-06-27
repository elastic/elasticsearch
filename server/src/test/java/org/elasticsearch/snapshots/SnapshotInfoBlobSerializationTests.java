/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.test.AbstractWireTestCase;

import java.io.IOException;

public class SnapshotInfoBlobSerializationTests extends AbstractWireTestCase<SnapshotInfo> {

    @Override
    protected SnapshotInfo createTestInstance() {
        return SnapshotInfoTestUtils.createRandomSnapshotInfo();
    }

    @Override
    protected SnapshotInfo mutateInstance(SnapshotInfo instance) throws IOException {
        return SnapshotInfoTestUtils.mutateSnapshotInfo(instance);
    }

    @Override
    protected SnapshotInfo copyInstance(SnapshotInfo instance, Version version) throws IOException {
        final PlainActionFuture<SnapshotInfo> future = new PlainActionFuture<>();
        BlobStoreRepository.SNAPSHOT_FORMAT.serialize(
            instance,
            "test",
            randomBoolean(),
            BigArrays.NON_RECYCLING_INSTANCE,
            bytes -> ActionListener.completeWith(
                future,
                () -> BlobStoreRepository.SNAPSHOT_FORMAT.deserialize(
                    instance.repository(),
                    NamedXContentRegistry.EMPTY,
                    bytes.streamInput()
                )
            )
        );
        return future.actionGet();
    }

}
