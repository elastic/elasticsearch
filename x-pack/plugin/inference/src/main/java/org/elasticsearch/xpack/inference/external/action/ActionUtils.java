/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.rest.RestStatus;

import java.util.List;

public class ActionUtils {

    public static ActionListener<List<? extends InferenceResults>> wrapFailuresInElasticsearchException(
        String errorMessage,
        ActionListener<List<? extends InferenceResults>> listener
    ) {
        return ActionListener.wrap(listener::onResponse, e -> {
            var unwrappedException = ExceptionsHelper.unwrapCause(e);

            if (unwrappedException instanceof ElasticsearchException esException) {
                listener.onFailure(esException);
            } else {
                listener.onFailure(createInternalServerError(unwrappedException, errorMessage));
            }
        });
    }

    public static ElasticsearchStatusException createInternalServerError(Throwable e, String message) {
        return new ElasticsearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR, e);
    }

    private ActionUtils() {}
}
