/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.status;

import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class SnapshotShardsStatsTests extends AbstractXContentTestCase<SnapshotShardsStats> {

    static final ConstructingObjectParser<SnapshotShardsStats, Void> PARSER = new ConstructingObjectParser<>(
        SnapshotShardsStats.Fields.SHARDS_STATS,
        true,
        (Object[] parsedObjects) -> {
            int i = 0;
            int initializingShards = (int) parsedObjects[i++];
            int startedShards = (int) parsedObjects[i++];
            int finalizingShards = (int) parsedObjects[i++];
            int doneShards = (int) parsedObjects[i++];
            int failedShards = (int) parsedObjects[i++];
            int totalShards = (int) parsedObjects[i];
            return new SnapshotShardsStats(initializingShards, startedShards, finalizingShards, doneShards, failedShards, totalShards);
        }
    );
    static {
        PARSER.declareInt(constructorArg(), new ParseField(SnapshotShardsStats.Fields.INITIALIZING));
        PARSER.declareInt(constructorArg(), new ParseField(SnapshotShardsStats.Fields.STARTED));
        PARSER.declareInt(constructorArg(), new ParseField(SnapshotShardsStats.Fields.FINALIZING));
        PARSER.declareInt(constructorArg(), new ParseField(SnapshotShardsStats.Fields.DONE));
        PARSER.declareInt(constructorArg(), new ParseField(SnapshotShardsStats.Fields.FAILED));
        PARSER.declareInt(constructorArg(), new ParseField(SnapshotShardsStats.Fields.TOTAL));
    }

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
        return PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
