package org.elasticsearch.protocol.xpack.watcher;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class DeactivateWatchResponseTests extends AbstractXContentTestCase<DeactivateWatchResponse> {
    @Override
    protected DeactivateWatchResponse createTestInstance() {
        String id = randomAlphaOfLength(10);
        long version = randomLongBetween(1, 10);
        // TODO: Randomized Status
        String status = randomAlphaOfLength(10);
        return new DeactivateWatchResponse(id, version, status);
    }

    @Override
    protected DeactivateWatchResponse doParseInstance(XContentParser parser) throws IOException {
        return DeactivateWatchResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
