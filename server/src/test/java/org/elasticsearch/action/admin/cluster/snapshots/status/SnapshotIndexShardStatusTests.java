/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.status;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.function.Predicate;

public class SnapshotIndexShardStatusTests extends AbstractXContentTestCase<SnapshotIndexShardStatus> {

    @Override
    protected SnapshotIndexShardStatus createTestInstance() {
        return createForIndex(randomAlphaOfLength(10));
    }

    protected SnapshotIndexShardStatus createForIndex(String indexName) {
        ShardId shardId = new ShardId(new Index(indexName, IndexMetadata.INDEX_UUID_NA_VALUE), randomIntBetween(0, 500));
        SnapshotIndexShardStage stage = randomFrom(SnapshotIndexShardStage.values());
        SnapshotStats stats = new SnapshotStatsTests().createTestInstance();
        String nodeId = randomAlphaOfLength(20);
        String failure = null;
        if (rarely()) {
            failure = randomAlphaOfLength(200);
        }
        return new SnapshotIndexShardStatus(shardId, stage, stats, nodeId, failure);
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // Do not place random fields in the root object since its fields correspond to shard names.
        return String::isEmpty;
    }

    @Override
    protected SnapshotIndexShardStatus doParseInstance(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser);
        SnapshotIndexShardStatus status = SnapshotIndexShardStatus.fromXContent(parser, parser.currentName());
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
        return status;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
