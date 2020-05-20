/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.audit;

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.common.Randomness;
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

    public static String generateRequestId(ThreadContext threadContext) {
        return generateRequestId(threadContext, true);
    }

    public static String getOrGenerateRequestId(ThreadContext threadContext) {
        final String requestId = extractRequestId(threadContext);
        if (Strings.isEmpty(requestId)) {
            return generateRequestId(threadContext, false);
        }
        return requestId;
    }

    private static String generateRequestId(ThreadContext threadContext, boolean checkExisting) {
        if (checkExisting) {
            final String existing = extractRequestId(threadContext);
            if (existing != null) {
                throw new IllegalStateException("Cannot generate a new audit request id - existing id ["
                    + existing + "] already registered");
            }
        }
        final String requestId = UUIDs.randomBase64UUID(Randomness.get());
        // Store as a header (not transient) so that it is passed over the network if this request requires execution on other nodes
        threadContext.putHeader(AUDIT_REQUEST_ID, requestId);
        return requestId;
    }

    public static String extractRequestId(ThreadContext threadContext) {
        return threadContext.getHeader(AUDIT_REQUEST_ID);
    }
}
