package org.elasticsearch.action.admin.cluster.snapshots.status;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

public class SnapshotsStatusResponseTests extends AbstractXContentTestCase<SnapshotsStatusResponse> {

    @Override
    protected SnapshotsStatusResponse doParseInstance(XContentParser parser) throws IOException {
        return SnapshotsStatusResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected SnapshotsStatusResponse createTestInstance() {
        SnapshotStatusTests statusBuilder = new SnapshotStatusTests();
        List<SnapshotStatus> snapshotStatuses = new ArrayList<>();
        for (int idx = 0; idx < randomIntBetween(0, 5); idx++) {
            snapshotStatuses.add(statusBuilder.createTestInstance());
        }
        return new SnapshotsStatusResponse(snapshotStatuses);
    }
}
