package org.elasticsearch.action.admin.cluster.snapshots.status;

import java.io.IOException;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.test.AbstractXContentTestCase;

public class SnapshotStatsTests extends AbstractXContentTestCase<SnapshotStats> {

    @Override
    protected SnapshotStats createTestInstance() {
        long startTime = randomNonNegativeLong();
        long time = randomNonNegativeLong();
        int incrementalFileCount = randomIntBetween(0, Integer.MAX_VALUE);
        int totalFileCount = randomIntBetween(0, Integer.MAX_VALUE);
        int processedFileCount = randomIntBetween(0, Integer.MAX_VALUE);
        long incrementalSize = ((long)randomIntBetween(0, Integer.MAX_VALUE)) * 2;
        long totalSize = ((long)randomIntBetween(0, Integer.MAX_VALUE)) * 2;
        long processedSize = ((long)randomIntBetween(0, Integer.MAX_VALUE)) * 2;
        return new SnapshotStats(startTime, time, incrementalFileCount, totalFileCount,
            processedFileCount, incrementalSize, totalSize, processedSize);
    }

    @Override
    protected SnapshotStats doParseInstance(XContentParser parser) throws IOException {
        // TODO: standards check
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser::getTokenLocation);
        SnapshotStats stats = SnapshotStats.fromXContent(parser);
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser::getTokenLocation);
        return stats;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
