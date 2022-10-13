/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.test.AbstractWireTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;

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
        final BytesStreamOutput out = new BytesStreamOutput();
        BlobStoreRepository.SNAPSHOT_FORMAT.serialize(instance, "test", randomBoolean(), out);
        return BlobStoreRepository.SNAPSHOT_FORMAT.deserialize(
            instance.repository(),
            NamedXContentRegistry.EMPTY,
            out.bytes().streamInput()
        );
    }

}
