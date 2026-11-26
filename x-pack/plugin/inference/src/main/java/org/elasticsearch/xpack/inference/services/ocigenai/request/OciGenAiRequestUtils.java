/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.request;

import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiServiceSettings;

import java.io.IOException;

public final class OciGenAiRequestUtils {

    static final String SERVING_MODE_FIELD = "servingMode";
    static final String SERVING_TYPE_FIELD = "servingType";
    static final String MODEL_ID_FIELD = "modelId";
    static final String ENDPOINT_ID_FIELD = "endpointId";
    static final String ON_DEMAND = "ON_DEMAND";
    static final String DEDICATED = "DEDICATED";
    static final String COMPARTMENT_ID_FIELD = "compartmentId";

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
