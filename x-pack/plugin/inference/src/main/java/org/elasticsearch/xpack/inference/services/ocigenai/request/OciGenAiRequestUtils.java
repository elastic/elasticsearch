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
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiModel;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiServiceSettings;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.time.Clock;

public final class OciGenAiRequestUtils {

    static final String SERVING_MODE_FIELD = "servingMode";
    static final String SERVING_TYPE_FIELD = "servingType";
    static final String MODEL_ID_FIELD = "modelId";
    static final String ENDPOINT_ID_FIELD = "endpointId";
    static final String ON_DEMAND = "ON_DEMAND";
    static final String DEDICATED = "DEDICATED";
    static final String COMPARTMENT_ID_FIELD = "compartmentId";

    /**
     * Signs the request with the model's API signing key, adding the {@code date}, {@code host}, {@code x-content-sha256} and
     * {@code authorization} headers. Must be called after the body has been set, because the body bytes and the content type are
     * part of the signed data.
     */
    public static void signRequest(SimpleHttpRequest httpPost, OciGenAiModel model) {
        var secretSettings = model.getSecretSettings();
        if (secretSettings == null) {
            throw new ElasticsearchStatusException(
                "Unable to sign the OCI Generative AI request from inference entity id [{}] because the API signing key is missing",
                RestStatus.BAD_REQUEST,
                model.getInferenceEntityId()
            );
        }

        try {
            var privateKey = OciGenAiPrivateKeyParser.parse(secretSettings.privateKey().toString());
            var signer = new OciGenAiRequestSigner(secretSettings.keyId(), privateKey, Clock.systemUTC());

            signer.sign(httpPost.getMethod(), httpPost.getUri(), contentType(httpPost), httpPost.getBodyBytes())
                .forEach(httpPost::setHeader);
        } catch (GeneralSecurityException | URISyntaxException e) {
            throw new ElasticsearchStatusException(
                "Failed to sign the OCI Generative AI request from inference entity id [{}]: {}",
                RestStatus.BAD_REQUEST,
                e,
                model.getInferenceEntityId(),
                e.getMessage()
            );
        }
    }

    /**
     * The {@code Content-Type} value the HTTP client will send: an explicitly set header wins, otherwise the content type of the
     * body, which the client adds as a header when none is present.
     */
    private static String contentType(SimpleHttpRequest httpPost) {
        var contentTypeHeader = httpPost.getFirstHeader(HttpHeaders.CONTENT_TYPE);
        if (contentTypeHeader != null) {
            return contentTypeHeader.getValue();
        }
        var body = httpPost.getBody();
        return body == null || body.getContentType() == null ? null : body.getContentType().toString();
    }

    /**
     * Writes the {@code compartmentId} and {@code servingMode} fields common to every OCI Generative AI inference request.
     */
    public static void writeCompartmentAndServingMode(XContentBuilder builder, OciGenAiServiceSettings serviceSettings) throws IOException {
        builder.field(COMPARTMENT_ID_FIELD, serviceSettings.compartmentId());
        builder.startObject(SERVING_MODE_FIELD);
        if (serviceSettings.isDedicated()) {
            builder.field(SERVING_TYPE_FIELD, DEDICATED);
            builder.field(ENDPOINT_ID_FIELD, serviceSettings.endpointId());
        } else {
            builder.field(SERVING_TYPE_FIELD, ON_DEMAND);
            builder.field(MODEL_ID_FIELD, serviceSettings.modelId());
        }
        builder.endObject();
    }

    private OciGenAiRequestUtils() {}
}
