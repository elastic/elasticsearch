/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.transform.notifications;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xpack.core.common.notifications.Level;

import java.util.Date;

import static org.hamcrest.Matchers.nullValue;

public class TransformAuditMessageTests extends AbstractXContentTestCase<TransformAuditMessage> {

    public void testGetJobType() {
        TransformAuditMessage message = createTestInstance();
        assertThat(message.getJobType(), nullValue());
    }

    @Override
    protected TransformAuditMessage doParseInstance(XContentParser parser) {
        return TransformAuditMessage.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected TransformAuditMessage createTestInstance() {
        return new TransformAuditMessage(
            randomBoolean() ? null : randomAlphaOfLength(10),
            randomAlphaOfLengthBetween(1, 20),
            randomFrom(Level.values()),
            new Date(),
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 20)
        );
    }
}
