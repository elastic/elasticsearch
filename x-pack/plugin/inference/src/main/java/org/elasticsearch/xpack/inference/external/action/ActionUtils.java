/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.rest.RestStatus;

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
                l.onFailure(
                    createInternalServerError(
                        unwrappedException,
                        Strings.format("%s. Cause: %s", errorMessage, unwrappedException.getMessage())
                    )
                );
            }
        });
    }

    public static ElasticsearchStatusException createInternalServerError(Throwable e, String message) {
        return new ElasticsearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR, e);
    }

    public static String constructFailedToSendRequestMessage(String message) {
        return Strings.format("Failed to send %s request", message);
    }

    private ActionUtils() {}
}
