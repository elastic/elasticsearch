package org.elasticsearch.action.admin.cluster.repositories.cleanup;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class CleanupRepositoryResponse extends ActionResponse implements ToXContentObject {
    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        return null;
    }
}
