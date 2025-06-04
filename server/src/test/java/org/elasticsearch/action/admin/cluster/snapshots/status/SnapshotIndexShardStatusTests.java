/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.status;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.function.Predicate;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

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
        SnapshotIndexShardStatus status = SnapshotIndexShardStatusTests.fromXContent(parser, parser.currentName());
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
        return status;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    static final ObjectParser.NamedObjectParser<SnapshotIndexShardStatus, String> PARSER;

    static {
        ConstructingObjectParser<SnapshotIndexShardStatus, ShardId> innerParser = new ConstructingObjectParser<>(
            "snapshot_index_shard_status",
            true,
            (Object[] parsedObjects, ShardId shard) -> {
                int i = 0;
                String rawStage = (String) parsedObjects[i++];
                String nodeId = (String) parsedObjects[i++];
                String failure = (String) parsedObjects[i++];
                SnapshotStats stats = (SnapshotStats) parsedObjects[i];

                SnapshotIndexShardStage stage;
                try {
                    stage = SnapshotIndexShardStage.valueOf(rawStage);
                } catch (IllegalArgumentException iae) {
                    throw new ElasticsearchParseException(
                        "failed to parse snapshot index shard status [{}][{}], unknown stage [{}]",
                        shard.getIndex().getName(),
                        shard.getId(),
                        rawStage
                    );
                }
                return new SnapshotIndexShardStatus(shard, stage, stats, nodeId, failure);
            }
        );
        innerParser.declareString(constructorArg(), new ParseField(SnapshotIndexShardStatus.Fields.STAGE));
        innerParser.declareString(optionalConstructorArg(), new ParseField(SnapshotIndexShardStatus.Fields.NODE));
        innerParser.declareString(optionalConstructorArg(), new ParseField(SnapshotIndexShardStatus.Fields.REASON));
        innerParser.declareObject(constructorArg(), (p, c) -> SnapshotStats.fromXContent(p), new ParseField(SnapshotStats.Fields.STATS));
        PARSER = (p, indexId, shardName) -> {
            // Combine the index name in the context with the shard name passed in for the named object parser
            // into a ShardId to pass as context for the inner parser.
            int shard;
            try {
                shard = Integer.parseInt(shardName);
            } catch (NumberFormatException nfe) {
                throw new ElasticsearchParseException(
                    "failed to parse snapshot index shard status [{}], expected numeric shard id but got [{}]",
                    indexId,
                    shardName
                );
            }
            ShardId shardId = new ShardId(new Index(indexId, IndexMetadata.INDEX_UUID_NA_VALUE), shard);
            return innerParser.parse(p, shardId);
        };
    }

    public static SnapshotIndexShardStatus fromXContent(XContentParser parser, String indexId) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
        return PARSER.parse(parser, indexId, parser.currentName());
    }
}
