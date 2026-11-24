/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.request;

import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.core5.http.HttpHeaders;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.common.secrets.SecretsApplier;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiSecretSettings;

import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.time.Clock;
import java.util.Objects;

/**
 * Signs outbound OCI Generative AI requests with the endpoint's API signing key. The PEM encoded private key is parsed once, when the
 * model is built from its stored secrets, rather than on every request: only the signature changes per request, because it covers the
 * {@code date} header. Updating the secrets rebuilds the model, and with it the applier, so a rotated key is picked up the same way.
 * <p>
 * A model may legitimately be built without usable signing key (no secrets, or a stored key that no longer parses), so construction never
 * fails; the problem is reported when a request is signed instead.
 */
public final class OciGenAiSecretsApplier implements SecretsApplier {

    private final String inferenceEntityId;
    @Nullable
    private final OciGenAiRequestSigner signer;
    @Nullable
    private final String signerUnavailableReason;

    public OciGenAiSecretsApplier(String inferenceEntityId, @Nullable OciGenAiSecretSettings secretSettings) {
        this(inferenceEntityId, secretSettings, Clock.systemUTC());
    }

    // visible for testing so that the date header can be controlled
    OciGenAiSecretsApplier(String inferenceEntityId, @Nullable OciGenAiSecretSettings secretSettings, Clock clock) {
        this.inferenceEntityId = Objects.requireNonNull(inferenceEntityId);
        Objects.requireNonNull(clock);

        OciGenAiRequestSigner parsedSigner = null;
        String unavailableReason = null;
        if (secretSettings == null) {
            unavailableReason = "the API signing key is missing";
        } else {
            try {
                var privateKey = OciGenAiPrivateKeyParser.parse(secretSettings.privateKey().toString());
                parsedSigner = new OciGenAiRequestSigner(secretSettings.keyId(), privateKey, clock);
            } catch (GeneralSecurityException e) {
                unavailableReason = "the API signing key is invalid: " + e.getMessage();
            }
        }
        this.signer = parsedSigner;
        this.signerUnavailableReason = unavailableReason;
    }

    /**
     * Adds the {@code date}, {@code host}, {@code x-content-sha256} and {@code authorization} headers. Must be called after the body has
     * been set, because the body bytes and the content type are part of the signed data.
     */
    @Override
    public void applyTo(SimpleHttpRequest request, ActionListener<SimpleHttpRequest> listener) {
        if (signer == null) {
            listener.onFailure(
                new ElasticsearchStatusException(
                    "Unable to sign the OCI Generative AI request from inference entity id [{}] because {}",
                    RestStatus.BAD_REQUEST,
                    inferenceEntityId,
                    signerUnavailableReason
                )
            );
            return;
        }

        try {
            signer.sign(request.getMethod(), request.getUri(), contentType(request), request.getBodyBytes()).forEach(request::setHeader);
        } catch (GeneralSecurityException | URISyntaxException e) {
            listener.onFailure(
                new ElasticsearchStatusException(
                    "Failed to sign the OCI Generative AI request from inference entity id [{}]: {}",
                    RestStatus.BAD_REQUEST,
                    e,
                    inferenceEntityId,
                    e.getMessage()
                )
            );
            return;
        }
        listener.onResponse(request);
    }

    /**
     * The {@code Content-Type} value the HTTP client will send: an explicitly set header wins, otherwise the content type of the
     * body, which the client adds as a header when none is present.
     */
    @Nullable
    private static String contentType(SimpleHttpRequest request) {
        var contentTypeHeader = request.getFirstHeader(HttpHeaders.CONTENT_TYPE);
        if (contentTypeHeader != null) {
            return contentTypeHeader.getValue();
        }
        var body = request.getBody();
        return body == null || body.getContentType() == null ? null : body.getContentType().toString();
    }
}
