/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.audit;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class AuditUtil {

    // We need to expose this to allow-list as a header passed for cross cluster requests; see `CrossClusterAccessServerTransportFilter`
    public static final String AUDIT_REQUEST_ID = "_xpack_audit_request_id";

    /**
     * Renders the body of {@code request} as a JSON string for inclusion in audit log events.
     * <p>
     * If {@code maxBytes > 0} and the rendered JSON length exceeds that limit, an
     * {@link ElasticsearchStatusException} with status 413 is thrown so the caller can reject
     * the request before it is written to the audit log.
     *
     * @param maxBytes   maximum allowed length of the rendered JSON string, in characters; {@code 0} = unlimited
     * @param settingKey the cluster setting key to include in the error message; may be {@code null}
     *                   when {@code maxBytes} is {@code 0}
     */
    public static String restRequestContent(RestRequest request, int maxBytes, String settingKey) {
        if (request.hasContent()) {
            var content = request.content();
            try {
                String json = XContentHelper.convertToJson(content, false, false, request.getXContentType());
                if (maxBytes > 0 && json.length() > maxBytes) {
                    throw new ElasticsearchStatusException(
                        "Request body size [{}] exceeds the audit body size limit [{}]; "
                            + "adjust the [{}] setting to increase the limit or set it to 0 to disable",
                        RestStatus.REQUEST_ENTITY_TOO_LARGE,
                        ByteSizeValue.ofBytes(json.length()),
                        ByteSizeValue.ofBytes(maxBytes),
                        settingKey
                    );
                }
                return json;
            } catch (ElasticsearchStatusException e) {
                throw e;
            } catch (Exception e) {
                return "Invalid Format: " + content.utf8ToString();
            }
        }
        return "";
    }

    public static Set<String> indices(TransportRequest message) {
        if (message instanceof IndicesRequest indicesRequest) {
            return arrayToSetOrNull(indicesRequest.indices());
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
                throw new IllegalStateException(
                    "Cannot generate a new audit request id - existing id [" + existing + "] already registered"
                );
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
