/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import static org.elasticsearch.snapshots.SnapshotInfoTestUtils.createRandomSnapshotInfo;
import static org.elasticsearch.snapshots.SnapshotInfoTestUtils.mutateSnapshotInfo;

public class SnapshotInfoWriteableTests extends AbstractWireSerializingTestCase<SnapshotInfo> {

    @Override
    protected SnapshotInfo createTestInstance() {
        return createRandomSnapshotInfo();
    }

    @Override
    protected Writeable.Reader<SnapshotInfo> instanceReader() {
        return SnapshotInfo::readFrom;
    }

    @Override
    protected SnapshotInfo mutateInstance(SnapshotInfo instance) {
        return mutateSnapshotInfo(instance);
    }
}
