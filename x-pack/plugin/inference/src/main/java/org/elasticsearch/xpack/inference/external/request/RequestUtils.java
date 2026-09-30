/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request;

import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;

public class RequestUtils {

    public static Header createAuthBearerHeader(SecureString apiKey) {
        return new BasicHeader(HttpHeaders.AUTHORIZATION, bearerToken(apiKey.toString()));
    }

    public static String bearerToken(String apiKey) {
        return "Bearer " + apiKey;
    }

    public static Header createAuthApiKeyHeader(SecureString apiKey) {
        return new BasicHeader(HttpHeaders.AUTHORIZATION, apiKey(apiKey.toString()));
    }

    public static String apiKey(String apiKey) {
        return "ApiKey " + apiKey;
    }

    public static URI buildUri(URI accountUri, String service, CheckedSupplier<URI, URISyntaxException> uriBuilder) {
        try {
            return accountUri == null ? uriBuilder.get() : accountUri;
        } catch (URISyntaxException e) {
            // using bad request here so that potentially sensitive URL information does not get logged
            throw new ElasticsearchStatusException(Strings.format("Failed to construct %s URL", service), RestStatus.BAD_REQUEST, e);
        }
    }

    public static URI buildUri(String service, CheckedSupplier<URI, URISyntaxException> uriBuilder) {
        return buildUri(null, service, uriBuilder);
    }

    /**
     * Serializes {@code entity} as a JSON request body, writing straight into a byte buffer rather than going through
     * {@link Strings#toString(org.elasticsearch.xcontent.ToXContent)}.
     * <p>
     * {@code Strings.toString} allocates an intermediate {@link String} holding the whole body and then a second copy when that string
     * is encoded back to bytes. For bodies carrying base64-encoded files (e.g. document extraction) the body is bounded only by
     * {@code http.max_content_length}, and the retrying sender re-serializes it on every attempt, so those copies are worth avoiding.
     * <p>
     * Unlike {@code Strings.toString}, which swallows an {@link IOException} and returns a JSON error object that would then be sent to
     * the third-party service as the request body, this fails the request.
     * <p>
     * Returns a {@link ByteArrayEntity} rather than a streaming entity on purpose: the entity must be repeatable for retries.
     */
    public static ByteArrayEntity jsonEntity(ToXContentObject entity) {
        try (var output = new BytesStreamOutput()) {
            try (var builder = XContentFactory.jsonBuilder(output)) {
                entity.toXContent(builder, ToXContent.EMPTY_PARAMS);
            }
            return new ByteArrayEntity(BytesReference.toBytes(output.bytes()));
        } catch (IOException e) {
            throw new UncheckedIOException(Strings.format("Failed to serialize [%s] request body", entity.getClass().getSimpleName()), e);
        }
    }

    /**
     * Sets the {@code Content-Type} and {@code Authorization: Bearer} headers on the given request using the supplied API key.
     */
    public static void decorateWithAuthHeader(HttpPost request, SecureString apiKey) {
        request.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType());
        request.setHeader(createAuthBearerHeader(apiKey));
    }

    private RequestUtils() {}
}
