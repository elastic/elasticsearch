/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.notifications;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xpack.core.common.notifications.Level;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.util.Date;

import static org.hamcrest.Matchers.equalTo;

public class AnomalyDetectionAuditMessageTests extends AbstractXContentTestCase<AnomalyDetectionAuditMessage> {

    public void testGetJobType() {
        AnomalyDetectionAuditMessage message = createTestInstance();
        assertThat(message.getJobType(), equalTo(Job.ANOMALY_DETECTOR_JOB_TYPE));
    }

    @Override
    protected AnomalyDetectionAuditMessage doParseInstance(XContentParser parser) {
        return AnomalyDetectionAuditMessage.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected AnomalyDetectionAuditMessage createTestInstance() {
        return new AnomalyDetectionAuditMessage(
            randomBoolean() ? null : randomAlphaOfLength(10),
            randomAlphaOfLengthBetween(1, 20),
            randomFrom(Level.values()),
            new Date(),
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 20)
        );
    }
}
