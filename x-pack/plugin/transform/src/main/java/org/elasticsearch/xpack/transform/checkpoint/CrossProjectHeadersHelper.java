/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.checkpoint;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * Minimal helper that runs {@link GetTransformCrossProjectHeadersAction} as a local-only internal action.
 */
public final class CrossProjectHeadersHelper {

    private static final Logger logger = LogManager.getLogger(CrossProjectHeadersHelper.class);

    private CrossProjectHeadersHelper() {}

    public static void executeWithCrossProjectHeaders(Client client, TransformConfig transformConfig, ActionListener<Void> listener) {
        // force fresh token by removing any existing request-scoped credential
        Map<String, String> currentHeaders = copyWithoutRequestScopedCredential(transformConfig.getHeaders());
        ClientHelper.executeWithHeadersAsync(
            currentHeaders,
            ClientHelper.TRANSFORM_ORIGIN,
            client,
            GetTransformCrossProjectHeadersAction.INSTANCE,
            new GetTransformCrossProjectHeadersAction.Request(transformConfig),
            ActionListener.wrap(r -> listener.onResponse(null), listener::onFailure)
        );
    }

    private static Map<String, String> copyWithoutRequestScopedCredential(Map<String, String> headers) {
        if (headers.containsKey("_security_serverless_request_scoped_credential") == false) {
            return headers;
        }
        var copy = new HashMap<>(headers);
        copy.remove("_security_serverless_request_scoped_credential");
        return copy;
    }

}
