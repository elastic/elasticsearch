/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.audit;

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.transport.TransportMessage;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class AuditUtil {

    private static final String AUDIT_REQUEST_ID = "_xpack_audit_request_id";

    public static String restRequestContent(RestRequest request) {
        if (request.hasContent()) {
            try {
                return XContentHelper.convertToJson(request.content(), false, false, request.getXContentType());
            } catch (IOException ioe) {
                return "Invalid Format: " + request.content().utf8ToString();
            }
        }
        return "";
    }

    public static Set<String> indices(TransportMessage message) {
        if (message instanceof IndicesRequest) {
            return arrayToSetOrNull(((IndicesRequest) message).indices());
        }
        return null;
    }

    private static Set<String> arrayToSetOrNull(String[] indices) {
        return indices == null ? null : new HashSet<>(Arrays.asList(indices));
    }

    public static String getOrGenerateRequestId(ThreadContext threadContext) {
        String requestId = extractRequestId(threadContext);
        if (Strings.isEmpty(requestId)) {
            requestId = UUIDs.randomBase64UUID();
            threadContext.putTransient(AUDIT_REQUEST_ID, requestId);
        }
        return requestId;
    }

    public static String extractRequestId(ThreadContext threadContext) {
        return threadContext.getTransient(AUDIT_REQUEST_ID);
    }
}
