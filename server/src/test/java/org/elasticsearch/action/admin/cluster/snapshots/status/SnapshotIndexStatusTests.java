package org.elasticsearch.action.admin.cluster.snapshots.status;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.test.AbstractXContentTestCase;


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
    protected SnapshotIndexStatus doParseInstance(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser::getTokenLocation);
        SnapshotIndexStatus status = SnapshotIndexStatus.fromXContent(parser);
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser::getTokenLocation);
        return status;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
