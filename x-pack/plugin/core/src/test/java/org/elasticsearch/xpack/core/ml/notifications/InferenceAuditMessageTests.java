/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.notifications;

import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.common.notifications.Level;

import java.util.Date;

public class InferenceAuditMessageTests extends AuditMessageTests<InferenceAuditMessage> {

    @Override
    public String getJobType() {
        return "inference";
    }

    @Override
    protected InferenceAuditMessage doParseInstance(XContentParser parser) {
        return InferenceAuditMessage.PARSER.apply(parser, null);
    }

    @Override
    protected InferenceAuditMessage createTestInstance() {
        return new InferenceAuditMessage(
            randomBoolean() ? null : randomAlphaOfLength(10),
            randomAlphaOfLengthBetween(1, 20),
            randomFrom(Level.values()),
            new Date(),
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 20)
        );
    }
}
