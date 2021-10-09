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

public class DataFrameAnalyticsAuditMessageTests extends AuditMessageTests<DataFrameAnalyticsAuditMessage> {

    @Override
    public String getJobType() {
        return "data_frame_analytics";
    }

    @Override
    protected DataFrameAnalyticsAuditMessage doParseInstance(XContentParser parser) {
        return DataFrameAnalyticsAuditMessage.PARSER.apply(parser, null);
    }

    @Override
    protected DataFrameAnalyticsAuditMessage createTestInstance() {
        return new DataFrameAnalyticsAuditMessage(
            randomBoolean() ? null : randomAlphaOfLength(10),
            randomAlphaOfLengthBetween(1, 20),
            randomFrom(Level.values()),
            new Date(),
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 20)
        );
    }
}
