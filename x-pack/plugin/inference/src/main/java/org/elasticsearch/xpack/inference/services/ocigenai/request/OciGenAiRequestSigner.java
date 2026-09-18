/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.request;

import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.Signature;
import java.time.Clock;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Signs HTTP requests with an OCI API signing key using the OCI request signature scheme, which is a profile of the
 * <a href="https://datatracker.ietf.org/doc/html/draft-cavage-http-signatures-08">draft-cavage HTTP Signatures</a> specification.
 * <p>
 * The {@code (request-target)}, {@code host} and {@code date} headers are always signed. For requests with a body ({@code POST},
 * {@code PUT}, {@code PATCH}) the {@code x-content-sha256}, {@code content-type} and {@code content-length} headers are signed as well.
 * The {@code content-length} header itself is not returned by {@link #sign(String, URI, String, byte[])} because HTTP clients set it
 * from the request entity; the signed value is the length of the supplied body, which is what the client will send for a non-chunked
 * byte array entity.
 *
 * @see <a href="https://docs.oracle.com/en-us/iaas/Content/API/Concepts/signingrequests.htm">OCI request signatures</a>
 */
public final class OciGenAiRequestSigner {

    public static final String SIGNATURE_VERSION = "1";
    public static final String ALGORITHM = "rsa-sha256";

    static final String REQUEST_TARGET_HEADER = "(request-target)";
    static final String HOST_HEADER = "host";
    static final String DATE_HEADER = "date";
    static final String CONTENT_SHA256_HEADER = "x-content-sha256";
    static final String CONTENT_TYPE_HEADER = "content-type";
    static final String CONTENT_LENGTH_HEADER = "content-length";
    static final String AUTHORIZATION_HEADER = "authorization";

    private static final Set<String> METHODS_WITH_BODY = Set.of("POST", "PUT", "PATCH");
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.RFC_1123_DATE_TIME.withLocale(Locale.ROOT)
        .withZone(ZoneOffset.UTC);
    private static final String JCA_SIGNATURE_ALGORITHM = "SHA256withRSA";

    private final String keyId;
    private final PrivateKey privateKey;
    private final Clock clock;

    /**
     * @param keyId      the OCI signing key identifier, {@code <tenancy ocid>/<user ocid>/<key fingerprint>} for API keys
     * @param privateKey the RSA private key of the API signing key pair
     * @param clock      the clock used to produce the {@code date} header
     */
    public OciGenAiRequestSigner(String keyId, PrivateKey privateKey, Clock clock) {
        this.keyId = Objects.requireNonNull(keyId);
        this.privateKey = Objects.requireNonNull(privateKey);
        this.clock = Objects.requireNonNull(clock);
    }

    /**
     * Computes the headers that authenticate the described request.
     *
     * @param method      the HTTP method
     * @param uri         the full request URL
     * @param contentType the value of the {@code Content-Type} header the client will send; required for requests with a body
     * @param body        the request body, or {@code null} for requests without one
     * @return the headers to set on the request, in insertion order: {@code date}, {@code host}, {@code x-content-sha256} (body
     *         requests only) and {@code authorization}
     */
    public Map<String, String> sign(String method, URI uri, @Nullable String contentType, @Nullable byte[] body)
        throws GeneralSecurityException {
        var upperCaseMethod = method.toUpperCase(Locale.ROOT);
        var hasBody = METHODS_WITH_BODY.contains(upperCaseMethod);

        var date = DATE_FORMATTER.format(clock.instant());
        var host = hostHeaderValue(uri);

        var signedHeaders = new LinkedHashMap<String, String>();
        signedHeaders.put(REQUEST_TARGET_HEADER, upperCaseMethod.toLowerCase(Locale.ROOT) + " " + requestTarget(uri));
        signedHeaders.put(HOST_HEADER, host);
        signedHeaders.put(DATE_HEADER, date);

        var headers = new LinkedHashMap<String, String>();
        headers.put(DATE_HEADER, date);
        headers.put(HOST_HEADER, host);

        if (hasBody) {
            var bodyBytes = body == null ? new byte[0] : body;
            var contentSha256 = Base64.getEncoder().encodeToString(MessageDigests.sha256().digest(bodyBytes));
            headers.put(CONTENT_SHA256_HEADER, contentSha256);
            signedHeaders.put(CONTENT_SHA256_HEADER, contentSha256);
            signedHeaders.put(
                CONTENT_TYPE_HEADER,
                Objects.requireNonNull(contentType, "a content type is required to sign a request body")
            );
            signedHeaders.put(CONTENT_LENGTH_HEADER, String.valueOf(bodyBytes.length));
        }

        var signature = signature(signingString(signedHeaders));
        headers.put(
            AUTHORIZATION_HEADER,
            Strings.format(
                "Signature version=\"%s\",keyId=\"%s\",algorithm=\"%s\",headers=\"%s\",signature=\"%s\"",
                SIGNATURE_VERSION,
                keyId,
                ALGORITHM,
                String.join(" ", signedHeaders.keySet()),
                signature
            )
        );
        return headers;
    }

    static String signingString(Map<String, String> signedHeaders) {
        return signedHeaders.entrySet().stream().map(entry -> entry.getKey() + ": " + entry.getValue()).collect(Collectors.joining("\n"));
    }

    static String requestTarget(URI uri) {
        var path = uri.getRawPath() == null || uri.getRawPath().isEmpty() ? "/" : uri.getRawPath();
        return uri.getRawQuery() == null ? path : path + "?" + uri.getRawQuery();
    }

    static String hostHeaderValue(URI uri) {
        return uri.getPort() == -1 ? uri.getHost() : uri.getHost() + ":" + uri.getPort();
    }

    private String signature(String signingString) throws GeneralSecurityException {
        var signature = Signature.getInstance(JCA_SIGNATURE_ALGORITHM);
        signature.initSign(privateKey);
        signature.update(signingString.getBytes(StandardCharsets.UTF_8));
        return Base64.getEncoder().encodeToString(signature.sign());
    }
}
