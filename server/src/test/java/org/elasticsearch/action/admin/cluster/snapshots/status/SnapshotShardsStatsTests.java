package org.elasticsearch.action.admin.cluster.snapshots.status;

import java.io.IOException;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.test.AbstractXContentTestCase;

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
        // TODO: standards check
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser::getTokenLocation);
        SnapshotShardsStats stats = SnapshotShardsStats.fromXContent(parser);
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser::getTokenLocation);
        return stats;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
