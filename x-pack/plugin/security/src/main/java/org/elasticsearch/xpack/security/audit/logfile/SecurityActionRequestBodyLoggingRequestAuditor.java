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

import java.io.IOException;

import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.ACTION_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.REQUEST_BODY_FIELD_NAME;

public class SecurityActionRequestBodyLoggingRequestAuditor implements RequestLoggingAuditTrail.RequestAuditor {

    public static final String NAME = "security_logfile";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void auditTransportRequest(String requestId, String action, TransportRequest transportRequest,
                                      LoggingAuditTrail.LogEntryBuilder logEntryBuilder) {
        // TODO extend this to include at least all manage security write actions/requests
        if (DeleteUserAction.NAME.equals(action) && transportRequest instanceof DeleteUserRequest) {
            try {
                XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.startObject()
                        .field("username", ((DeleteUserRequest) transportRequest).username())
                        .endObject();
                logEntryBuilder.withRequestId(requestId)
                        .with(ACTION_FIELD_NAME, action)
                        .with(REQUEST_BODY_FIELD_NAME, Strings.toString(builder));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
