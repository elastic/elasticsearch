/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.notifications;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditMessage;
import org.elasticsearch.xpack.core.common.notifications.Level;


import java.io.IOException;
import java.util.Date;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public abstract class AuditMessageTests<T extends AbstractAuditMessage> extends AbstractXContentTestCase<T> {

    public abstract String getJobType();

    public void testGetJobType() {
        AbstractAuditMessage message = createTestInstance();
        assertThat(message.getJobType(), equalTo(getJobType()));
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    public void testLongMessageIsTruncated() throws IOException {
        AbstractAuditMessage longMessage = new AbstractAuditMessage(
            randomBoolean() ? null : randomAlphaOfLength(10),
            "thisis17charslong".repeat(490),
            randomFrom(Level.values()),
            new Date(),
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 20)
        ) {
            @Override
            public String getJobType() {
                return "unused";
            }

            @Override
            protected String getResourceField() {
                return "unused";
            }
        };

        assertThat(longMessage.getMessage().length(), greaterThan(AbstractAuditMessage.MAX_AUDIT_MESSAGE_CHARS));

        // serialise the message and check the new message is truncated
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference originalXContent = XContentHelper.toXContent(longMessage, xContentType, randomBoolean());
        XContentParser parser = createParser(XContentFactory.xContent(xContentType), originalXContent);
        AbstractAuditMessage parsed = doParseInstance(parser);
        assertThat(parsed.getMessage().length(), equalTo(AbstractAuditMessage.MAX_AUDIT_MESSAGE_CHARS));
    }
}
