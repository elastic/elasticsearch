/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.notifications;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditMessage;
import org.elasticsearch.xpack.core.common.notifications.Level;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.util.Date;


public class InferenceAuditMessage extends AbstractAuditMessage {

    //TODO this should be MODEL_ID...
    private static final ParseField JOB_ID = Job.ID;
    public static final ConstructingObjectParser<InferenceAuditMessage, Void> PARSER =
        createParser("ml_inference_audit_message", InferenceAuditMessage::new, JOB_ID);

    public InferenceAuditMessage(String resourceId, String message, Level level, Date timestamp, String nodeName) {
        super(resourceId, message, level, timestamp, nodeName);
    }

    @Override
    public final String getJobType() {
        return "inference";
    }

    @Override
    protected String getResourceField() {
        return JOB_ID.getPreferredName();
    }
}
