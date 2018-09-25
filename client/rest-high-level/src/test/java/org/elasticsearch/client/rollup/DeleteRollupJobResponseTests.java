package org.elasticsearch.client.rollup;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.junit.Before;

import java.io.IOException;

public class DeleteRollupJobResponseTests extends AbstractXContentTestCase<DeleteRollupJobResponse> {

    private boolean acknowledged;

    @Before
    public void setupJobID() {
        acknowledged = randomBoolean();
    }

    @Override
    protected DeleteRollupJobResponse createTestInstance() {
        return new DeleteRollupJobResponse(acknowledged);
    }

    @Override
    protected  DeleteRollupJobResponse doParseInstance(XContentParser parser) throws IOException {
        return DeleteRollupJobResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

}
