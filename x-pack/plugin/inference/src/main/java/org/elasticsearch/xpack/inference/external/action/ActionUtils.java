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
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.rest.RestStatus;

import java.net.URI;

public class ActionUtils {

    public static ActionListener<InferenceServiceResults> wrapFailuresInElasticsearchException(
        String errorMessage,
        ActionListener<InferenceServiceResults> listener
    ) {
        return listener.delegateResponse((l, e) -> {
            var unwrappedException = ExceptionsHelper.unwrapCause(e);

            if (unwrappedException instanceof ElasticsearchException esException) {
                l.onFailure(esException);
            } else {
                l.onFailure(createInternalServerError(unwrappedException, errorMessage));
            }
        });
    }

    public static ElasticsearchStatusException createInternalServerError(Throwable e, String message) {
        return new ElasticsearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR, e);
    }

    public static String constructFailedToSendRequestMessage(@Nullable URI uri, String message) {
        if (uri != null) {
            return Strings.format("Failed to send %s request to [%s]", message, uri);
        }

        return Strings.format("Failed to send %s request", message);
    }

    private ActionUtils() {}
}
