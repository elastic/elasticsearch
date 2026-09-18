/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiModel;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiServiceSettings;

import java.io.IOException;
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
     * {@code authorization} headers. Must be called after the entity and the {@code Content-Type} header have been set.
     */
    public static void signRequest(HttpPost httpPost, OciGenAiModel model) {
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

            var contentTypeHeader = httpPost.getFirstHeader(HttpHeaders.CONTENT_TYPE);
            var contentType = contentTypeHeader == null ? null : contentTypeHeader.getValue();
            var body = httpPost.getEntity() == null ? null : EntityUtils.toByteArray(httpPost.getEntity());

            signer.sign(httpPost.getMethod(), httpPost.getURI(), contentType, body).forEach(httpPost::setHeader);
        } catch (GeneralSecurityException | IOException e) {
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
