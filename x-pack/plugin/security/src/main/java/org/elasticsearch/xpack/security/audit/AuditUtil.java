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
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xcontent.XContentType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class AuditUtil {

    private static final Logger logger = LogManager.getLogger(AuditUtil.class);

    // We need to expose this to allow-list as a header passed for cross cluster requests; see `CrossClusterAccessServerTransportFilter`
    public static final String AUDIT_REQUEST_ID = "_xpack_audit_request_id";

    public static String restRequestContent(RestRequest request, int maxBytes, @Nullable CircuitBreaker circuitBreaker, String settingKey) {
        if (request.hasContent()) {
            var content = request.content();
            final XContentType xContentType = request.getXContentType();
            if (xContentType == null) {
                final var parsedContentType = request.getParsedContentType();
                final String mediaType = parsedContentType != null ? parsedContentType.mediaTypeWithoutParameters() : "unknown";
                return "Unrecognized content type [" + mediaType + "]";
            }
            try {
                return XContentHelper.convertToJson(
                    content,
                    false,
                    false,
                    xContentType,
                    new XContentHelper.Budget(maxBytes, circuitBreaker, circuitBreaker != null ? "audit-body" : null)
                );
            } catch (XContentHelper.TooLargeBodyException e) {
                throw new ElasticsearchStatusException(
                    "Request body would exceed the audit size limit of [{}]; "
                        + "adjust [{}] to increase the limit or set it to 0 to disable",
                    RestStatus.REQUEST_ENTITY_TOO_LARGE,
                    ByteSizeValue.ofBytes(maxBytes),
                    settingKey
                );
            } catch (CircuitBreakingException e) {
                throw e;
            } catch (Exception e) {
                logger.warn(() -> Strings.format("failed to read body of REST request [%s] for auditing", request.uri()), e);
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
