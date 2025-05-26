/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.common.notifications.Level;

import java.io.IOException;
import java.util.Objects;

/**
 * Action to create ml notification messages from outside the
 * ml plugin. Designed for internal use the action is not
 * exposed via a REST endpoint. Within the ml plugin the Audit
 * classes should be used directly.
 */
public class AuditMlNotificationAction extends ActionType<AcknowledgedResponse> {

    public static final AuditMlNotificationAction INSTANCE = new AuditMlNotificationAction();
    public static final String NAME = "cluster:internal/xpack/ml/notification";

    private AuditMlNotificationAction() {
        super(NAME);
    }

    public enum AuditType {
        ANOMALY_DETECTION,
        DATAFRAME_ANALYTICS,
        INFERENCE,
        SYSTEM
    }

    public static class Request extends LegacyActionRequest {
        private final AuditType auditType;
        private final String id;
        private final String message;
        private final Level level;

        public Request(AuditType auditType, String id, String message, Level level) {
            this.auditType = Objects.requireNonNull(auditType);
            this.id = Objects.requireNonNull(id);
            this.message = Objects.requireNonNull(message);
            this.level = Objects.requireNonNull(level);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.auditType = in.readEnum(AuditType.class);
            this.id = in.readString();
            this.message = in.readString();
            this.level = in.readEnum(Level.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeEnum(auditType);
            out.writeString(id);
            out.writeString(message);
            out.writeEnum(level);
        }

        public AuditType getAuditType() {
            return auditType;
        }

        public String getId() {
            return id;
        }

        public String getMessage() {
            return message;
        }

        public Level getLevel() {
            return level;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(id, request.id)
                && Objects.equals(message, request.message)
                && auditType == request.auditType
                && level == request.level;
        }

        @Override
        public int hashCode() {
            return Objects.hash(auditType, id, message, level);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }
}
