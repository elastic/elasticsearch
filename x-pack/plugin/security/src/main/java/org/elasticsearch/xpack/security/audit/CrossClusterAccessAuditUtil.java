/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.audit;

import joptsimple.internal.Strings;

import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.Set;

public class CrossClusterAccessAuditUtil {

    public static Set<String> AUDIT_HEADERS_TO_COPY = Set.of(AuditTrail.X_FORWARDED_FOR_HEADER, AuditUtil.AUDIT_REQUEST_ID);

    public static ThreadContext.StoredContext stashContextWithAuditHeaders(final ThreadContext threadContext) {
        final String requestId = AuditUtil.extractRequestId(threadContext);
        final String xForwardedFor = threadContext.getHeader(AuditTrail.X_FORWARDED_FOR_HEADER);
        final ThreadContext.StoredContext stashed = threadContext.stashContext();
        if (false == Strings.isNullOrEmpty(requestId)) {
            AuditUtil.putRequestId(threadContext, requestId);
        }
        if (false == Strings.isNullOrEmpty(xForwardedFor)) {
            threadContext.putHeader(AuditTrail.X_FORWARDED_FOR_HEADER, xForwardedFor);
        }
        return stashed;
    }

}
