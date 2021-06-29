/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.status;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class SnapshotIndexStatusTests extends AbstractXContentTestCase<SnapshotIndexStatus> {

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
        SnapshotIndexStatus status = SnapshotIndexStatus.fromXContent(parser);
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
        return status;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
