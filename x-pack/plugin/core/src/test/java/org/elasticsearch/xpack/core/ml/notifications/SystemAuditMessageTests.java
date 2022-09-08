/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.notifications;

import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.common.notifications.Level;

import java.io.IOException;
import java.util.Date;

public class SystemAuditMessageTests extends AuditMessageTests<SystemAuditMessage> {

    @Override
    protected SystemAuditMessage createTestInstance() {
        return new SystemAuditMessage(
            randomAlphaOfLengthBetween(1, 20),
            randomFrom(Level.values()),
            new Date(),
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 20)
        );
    }

    @Override
    protected SystemAuditMessage doParseInstance(XContentParser parser) throws IOException {
        return SystemAuditMessage.PARSER.apply(parser, null);
    }

    @Override
    public String getJobType() {
        return "system";
    }
}
