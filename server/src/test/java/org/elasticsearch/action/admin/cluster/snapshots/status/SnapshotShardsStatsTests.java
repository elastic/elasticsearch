/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.status;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class SnapshotShardsStatsTests extends AbstractXContentTestCase<SnapshotShardsStats> {

    @Override
    protected SnapshotShardsStats createTestInstance() {
        int initializingShards = randomInt();
        int startedShards = randomInt();
        int finalizingShards = randomInt();
        int doneShards = randomInt();
        int failedShards = randomInt();
        int totalShards = randomInt();
        return new SnapshotShardsStats(initializingShards, startedShards, finalizingShards, doneShards, failedShards, totalShards);
    }

    @Override
    protected SnapshotShardsStats doParseInstance(XContentParser parser) throws IOException {
        return SnapshotShardsStats.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
