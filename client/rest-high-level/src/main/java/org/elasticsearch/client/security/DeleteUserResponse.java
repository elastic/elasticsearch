package org.elasticsearch.client.security;

import org.elasticsearch.client.core.AcknowledgedResponse;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * Response for a user being deleted from the native realm
 */
public final class DeleteUserResponse extends AcknowledgedResponse {

    private static final String PARSE_FIELD_NAME = "found";

    private static final ConstructingObjectParser<DeleteUserResponse, Void> PARSER = AcknowledgedResponse
        .generateParser("delete_user_response", DeleteUserResponse::new, PARSE_FIELD_NAME);

    public DeleteUserResponse(boolean acknowledged) {
        super(acknowledged);
    }

    public static DeleteUserResponse fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    protected String getFieldName() {
        return PARSE_FIELD_NAME;
    }
}
