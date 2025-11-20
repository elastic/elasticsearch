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
import org.elasticsearch.inference.InferenceServiceResults;

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
                    // Determine the appropriate RestStatus from the unwrapped exception, then wrap in an ElasticsearchStatusException
                    new ElasticsearchStatusException(
                        Strings.format("%s. Cause: %s", errorMessage, unwrappedException.getMessage()),
                        ExceptionsHelper.status(unwrappedException),
                        unwrappedException
                    )
                );
            }
        });
    }

    public static String constructFailedToSendRequestMessage(String message) {
        return Strings.format("Failed to send %s request", message);
    }

    private ActionUtils() {}
}
