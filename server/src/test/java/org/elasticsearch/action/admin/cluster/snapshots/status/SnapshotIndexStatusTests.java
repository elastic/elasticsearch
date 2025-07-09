/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.status;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class SnapshotIndexStatusTests extends AbstractXContentTestCase<SnapshotIndexStatus> {

    static final ObjectParser.NamedObjectParser<SnapshotIndexStatus, Void> PARSER;
    static {
        ConstructingObjectParser<SnapshotIndexStatus, String> innerParser = new ConstructingObjectParser<>(
            "snapshot_index_status",
            true,
            (Object[] parsedObjects, String index) -> {
                int i = 0;
                SnapshotShardsStats shardsStats = ((SnapshotShardsStats) parsedObjects[i++]);
                SnapshotStats stats = ((SnapshotStats) parsedObjects[i++]);
                @SuppressWarnings("unchecked")
                List<SnapshotIndexShardStatus> shardStatuses = (List<SnapshotIndexShardStatus>) parsedObjects[i];

                final Map<Integer, SnapshotIndexShardStatus> indexShards;
                if (shardStatuses == null || shardStatuses.isEmpty()) {
                    indexShards = emptyMap();
                } else {
                    indexShards = Maps.newMapWithExpectedSize(shardStatuses.size());
                    for (SnapshotIndexShardStatus shardStatus : shardStatuses) {
                        indexShards.put(shardStatus.getShardId().getId(), shardStatus);
                    }
                }
                return new SnapshotIndexStatus(index, indexShards, shardsStats, stats);
            }
        );
        innerParser.declareObject(
            constructorArg(),
            (p, c) -> SnapshotShardsStatsTests.PARSER.apply(p, null),
            new ParseField(SnapshotShardsStats.Fields.SHARDS_STATS)
        );
        innerParser.declareObject(
            constructorArg(),
            (p, c) -> SnapshotStatsTests.fromXContent(p),
            new ParseField(SnapshotStats.Fields.STATS)
        );
        innerParser.declareNamedObjects(
            constructorArg(),
            SnapshotIndexShardStatusTests.PARSER,
            new ParseField(SnapshotIndexStatus.Fields.SHARDS)
        );
        PARSER = ((p, c, name) -> innerParser.apply(p, name));
    }

    @Override
    protected SnapshotIndexStatus createTestInstance() {
        String index = randomAlphaOfLength(10);
        List<SnapshotIndexShardStatus> shardStatuses = new ArrayList<>();
        SnapshotIndexShardStatusTests builder = new SnapshotIndexShardStatusTests();
        for (int idx = 0; idx < randomIntBetween(0, 10); idx++) {
            shardStatuses.add(builder.createForIndex(index));
        }
        return new SnapshotIndexStatus(index, shardStatuses);
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // Do not place random fields in the root object or the shards field since their fields correspond to names.
        return (s) -> s.isEmpty() || s.endsWith("shards");
    }

    @Override
    protected SnapshotIndexStatus doParseInstance(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser);
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
        SnapshotIndexStatus status = PARSER.parse(parser, null, parser.currentName());
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
        return status;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
