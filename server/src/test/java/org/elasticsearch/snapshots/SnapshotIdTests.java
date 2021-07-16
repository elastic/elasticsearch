/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

public class SnapshotIdTests extends AbstractSerializingTestCase<SnapshotId> {

    @Override
    protected SnapshotId doParseInstance(XContentParser parser) throws IOException {
        return SnapshotId.parse(parser);
    }

    @Override
    protected Writeable.Reader<SnapshotId> instanceReader() {
        return SnapshotId::new;
    }

    @Override
    protected SnapshotId createTestInstance() {
        return new SnapshotId(randomSnapshotName(), randomSnapshotUuid());
    }

    @Override
    protected SnapshotId mutateInstance(SnapshotId instance) throws IOException {
        if (randomBoolean()) {
            return new SnapshotId(randomValueOtherThan(instance.getName(), SnapshotIdTests::randomSnapshotName), instance.getUUID());
        } else {
            return new SnapshotId(instance.getName(), randomValueOtherThan(instance.getUUID(), SnapshotIdTests::randomSnapshotUuid));
        }
    }

    private static String randomSnapshotName() {
        return randomAlphaOfLengthBetween(1, 10);
    }

    private static String randomSnapshotUuid() {
        return UUIDs.randomBase64UUID();
    }
}
