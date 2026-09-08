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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xcontent.XContentType;

import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.Set;

public class AuditUtil {

    private static final Logger logger = LogManager.getLogger(AuditUtil.class);

    // We need to expose this to allow-list as a header passed for cross cluster requests; see `CrossClusterAccessServerTransportFilter`
    public static final String AUDIT_REQUEST_ID = "_xpack_audit_request_id";

    private static final String PROTOBUF_MEDIA_TYPE = "application/x-protobuf";

    /**
     * Renders the body of {@code request} as a JSON string for inclusion in audit log events.
     * <p>
     * If {@code maxBytes > 0} and the rendered JSON length exceeds that limit, an
     * {@link ElasticsearchStatusException} with status 413 is thrown so the caller can reject
     * the request before it is written to the audit log. If the request has no XContent type
     * (e.g. a protobuf endpoint), a short diagnostic string is returned rather than throwing.
     *
     * @param maxBytes   maximum allowed length of the rendered JSON string, in characters; {@code 0} = unlimited
     * @param settingKey the cluster setting key to include in the error message; may be {@code null}
     *                   when {@code maxBytes} is {@code 0}
     */
    public static String restRequestContent(RestRequest request, int maxBytes, String settingKey) {
        if (request.hasContent()) {
            var content = request.content();
            try {
                final XContentType xContentType = request.getXContentType();
                if (xContentType == null) {
                    final var parsedContentType = request.getParsedContentType();
                    final String mediaType = parsedContentType != null ? parsedContentType.mediaTypeWithoutParameters() : "unknown";
                    return "Unrecognized content type [" + mediaType + "]";
                }
                String json = XContentHelper.convertToJson(content, false, false, xContentType);
                if (maxBytes > 0 && json.length() > maxBytes) {
                    throw bodyExceedsLimit(json.length(), maxBytes, settingKey);
                }
                return json;
            } catch (ElasticsearchStatusException e) {
                throw e;
            } catch (Exception e) {
                logger.warn(() -> Strings.format("failed to read body of REST request [%s] for auditing", request.uri()), e);
                return "Invalid Format: " + content.utf8ToString();
            }
        }
        return "";
    }

    /**
     * Returns true if the request has a body whose declared media type is protobuf.
     */
    public static boolean hasProtobufContent(RestRequest request) {
        return request.hasContent()
            && request.getParsedContentType() != null
            && PROTOBUF_MEDIA_TYPE.equals(request.getParsedContentType().mediaTypeWithoutParameters());
    }

    /**
     * Returns the body of {@code request} base64-encoded, for bodies that have no JSON representation (see {@link #hasProtobufContent}).
     * The bytes are encoded as received by the handler. Any compression that the HTTP layer does not decompress (for example snappy for
     * Prometheus remote-write) is preserved verbatim.
     * <p>
     * Callers must ensure the request has content. The sole production caller gates on {@link #hasProtobufContent} which requires it.
     *
     * @param maxBytes   maximum allowed length of the base64 string, in characters. {@code 0} means unlimited. The limit is checked
     *                   before encoding so an oversized body is rejected without the encoding allocation.
     * @param settingKey the cluster setting key to include in the error message. May be {@code null} when {@code maxBytes} is {@code 0}.
     */
    public static String restRequestRawContent(RestRequest request, int maxBytes, String settingKey) {
        assert request.hasContent() : "restRequestRawContent requires a request with content";
        var content = request.content();
        long encodedLength = base64EncodedLength(content.length());
        if (maxBytes > 0 && encodedLength > maxBytes) {
            throw bodyExceedsLimit(encodedLength, maxBytes, settingKey);
        }
        return Base64.getEncoder().encodeToString(BytesReference.toBytes(content));
    }

    /**
     * Returns the length, in characters, of the padded RFC 4648 base64 encoding of {@code sourceLength} bytes: every 3 input bytes are
     * encoded as 4 output characters, with the last group padded to a multiple of 4.
     */
    static long base64EncodedLength(int sourceLength) {
        return 4L * ((sourceLength + 2L) / 3L);
    }

    private static ElasticsearchStatusException bodyExceedsLimit(long size, int maxBytes, String settingKey) {
        return new ElasticsearchStatusException(
            "Request body size [{}] exceeds the audit body size limit [{}]; "
                + "adjust the [{}] setting to increase the limit or set it to 0 to disable",
            RestStatus.REQUEST_ENTITY_TOO_LARGE,
            ByteSizeValue.ofBytes(size),
            ByteSizeValue.ofBytes(maxBytes),
            settingKey
        );
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
