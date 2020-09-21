/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.audit.logfile;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.action.user.DeleteUserAction;
import org.elasticsearch.xpack.core.security.action.user.DeleteUserRequest;
import org.elasticsearch.xpack.security.audit.TransportRequestAuditTrail;

import java.io.IOException;

public class SecurityActionRequestBodyLoggingAuditor implements TransportRequestAuditTrail.Auditor {

    public static final String NAME = "security_logfile";

    private final LoggingAuditTrail loggingAuditTrail;

    public SecurityActionRequestBodyLoggingAuditor(LoggingAuditTrail loggingAuditTrail) {
        this.loggingAuditTrail = loggingAuditTrail;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void auditTransportRequest(String requestId, String action, TransportRequest transportRequest) {
        if (DeleteUserAction.NAME.equals(action) && transportRequest instanceof DeleteUserRequest) {
            try {
                XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.startObject()
                        .field("username", ((DeleteUserRequest) transportRequest).username())
                        .endObject();
                loggingAuditTrail.logRequestBody(requestId, action, Strings.toString(builder));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
