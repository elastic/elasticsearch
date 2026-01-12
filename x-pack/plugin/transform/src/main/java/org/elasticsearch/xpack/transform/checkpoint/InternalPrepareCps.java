/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.checkpoint;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;

/**
 * Minimal helper that runs {@link InternalPrepareCpsAction} as a local-only internal action.
 */
public final class InternalPrepareCps {

    private InternalPrepareCps() {}

    public static void execute(ParentTaskAssigningClient client, TransformConfig transformConfig, ActionListener<Void> listener) {
        ClientHelper.executeWithHeadersAsync(
            transformConfig.getHeaders(),
            ClientHelper.TRANSFORM_ORIGIN,
            client,
            InternalPrepareCpsAction.INSTANCE,
            new InternalPrepareCpsAction.Request(transformConfig),
            ActionListener.wrap(r -> listener.onResponse(null), listener::onFailure)
        );
    }
}
