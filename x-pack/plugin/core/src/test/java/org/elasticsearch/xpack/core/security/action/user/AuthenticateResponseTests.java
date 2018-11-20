package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.protocol.AbstractHlrcStreamableXContentTestCase;

import java.io.IOException;

public class AuthenticateResponseTests extends AbstractHlrcStreamableXContentTestCase<AuthenticateResponse,
    org.elasticsearch.client.security.AuthenticateResponse> {
    @Override
    public org.elasticsearch.client.security.AuthenticateResponse doHlrcParseInstance(XContentParser parser) throws IOException {
        return null;
    }

    @Override
    public AuthenticateResponse convertHlrcToInternal(org.elasticsearch.client.security.AuthenticateResponse instance) {
        return null;
    }

    @Override
    protected AuthenticateResponse createBlankInstance() {
        return null;
    }

    @Override
    protected AuthenticateResponse createTestInstance() {
        return null;
    }
}
