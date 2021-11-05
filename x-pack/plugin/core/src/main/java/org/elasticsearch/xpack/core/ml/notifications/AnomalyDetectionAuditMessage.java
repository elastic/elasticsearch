/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.notifications;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditMessage;
import org.elasticsearch.xpack.core.common.notifications.Level;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.util.Date;

public class AnomalyDetectionAuditMessage extends AbstractAuditMessage {

    private static final ParseField JOB_ID = Job.ID;
    public static final ConstructingObjectParser<AnomalyDetectionAuditMessage, Void> PARSER = createParser(
        "ml_audit_message",
        AnomalyDetectionAuditMessage::new,
        JOB_ID
    );

    public AnomalyDetectionAuditMessage(String resourceId, String message, Level level, Date timestamp, String nodeName) {
        super(resourceId, message, level, timestamp, nodeName);
    }

    @Override
    public final String getJobType() {
        return Job.ANOMALY_DETECTOR_JOB_TYPE;
    }

    @Override
    protected String getResourceField() {
        return JOB_ID.getPreferredName();
    }
}
