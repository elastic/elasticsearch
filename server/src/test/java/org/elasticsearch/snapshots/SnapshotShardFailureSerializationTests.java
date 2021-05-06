/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.snapshots;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class SnapshotShardFailureSerializationTests extends AbstractXContentTestCase<SnapshotShardFailure> {

    @Override
    protected SnapshotShardFailure createTestInstance() {
        return new SnapshotShardFailure(
                UUIDs.randomBase64UUID(random()),
                new ShardId(
                        randomAlphaOfLengthBetween(5, 10),
                        UUIDs.randomBase64UUID(random()),
                        randomNonNegativeByte()
                ),
                randomAlphaOfLengthBetween(1, 100)
        );
    }

    @Override
    protected SnapshotShardFailure doParseInstance(XContentParser parser) throws IOException {
        return SnapshotShardFailure.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
