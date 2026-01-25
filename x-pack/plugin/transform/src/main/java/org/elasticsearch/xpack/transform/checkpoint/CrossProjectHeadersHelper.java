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

/**
 * Minimal helper that runs {@link GetCrossProjectHeadersAction} as a local-only internal action.
 */
public final class CrossProjectHeadersHelper {

    private static final Logger logger = LogManager.getLogger(CrossProjectHeadersHelper.class);

    private CrossProjectHeadersHelper() {}

    public static void executeWithCrossProjectHeaders(Client client, TransformConfig transformConfig, ActionListener<Void> listener) {
        ClientHelper.executeWithHeadersAsync(
            transformConfig.getHeaders(),
            ClientHelper.TRANSFORM_ORIGIN,
            client,
            GetCrossProjectHeadersAction.INSTANCE,
            new GetCrossProjectHeadersAction.Request(transformConfig),
            ActionListener.wrap(r -> listener.onResponse(null), listener::onFailure)
        );
    }
}
