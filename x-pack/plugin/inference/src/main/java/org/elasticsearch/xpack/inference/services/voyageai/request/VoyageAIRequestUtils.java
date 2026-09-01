/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.request;

import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIModel;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;

public final class VoyageAIRequestUtils {

    public static void decorateWithHeaders(SimpleHttpRequest request, VoyageAIModel model) {
        request.setHeader(createAuthBearerHeader(model.getSecretSettings().apiKey()));
        request.setHeader(VoyageAIUtils.createRequestSourceHeader());
    }

    private VoyageAIRequestUtils() {}
}
