/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.request;

import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.elasticsearch.common.settings.SecureString;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;

public final class JinaAIRequestUtils {

    public static void decorateWithAuthHeader(SimpleHttpRequest request, SecureString apiKey) {
        request.setHeader(createAuthBearerHeader(apiKey));
        request.setHeader(JinaAIUtils.createRequestSourceHeader());
    }

    private JinaAIRequestUtils() {}
}
