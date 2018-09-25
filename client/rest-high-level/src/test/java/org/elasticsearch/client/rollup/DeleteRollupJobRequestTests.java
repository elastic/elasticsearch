package org.elasticsearch.client.rollup;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.junit.Before;

import java.io.IOException;

public class DeleteRollupJobRequestTests extends AbstractXContentTestCase<DeleteRollupJobRequest> {

    private String jobId;

    @Before
    public void setUpOptionalId() {
        jobId = randomAlphaOfLengthBetween(1, 10);
    }

    @Override
    protected DeleteRollupJobRequest createTestInstance() {
        return new DeleteRollupJobRequest(jobId);
    }

    @Override
    protected DeleteRollupJobRequest doParseInstance(final XContentParser parser) throws IOException {
        return DeleteRollupJobRequest.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    public void testRequireConfiguration() {
        final NullPointerException e = expectThrows(NullPointerException.class, ()-> new DeleteRollupJobRequest(null));
        assertEquals("id parameter must not be null", e.getMessage());
    }

}
