/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.validation;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.rest.RestStatus;

public interface ServiceIntegrationValidator {
    void validate(InferenceService service, Model model, TimeValue timeout, ActionListener<InferenceServiceResults> listener);

    static ActionListener<InferenceServiceResults> wrapListenerForValidation(ActionListener<InferenceServiceResults> listener) {
        return ActionListener.wrap(r -> {
            if (r != null) {
                listener.onResponse(r);
            } else {
                listener.onFailure(
                    new ElasticsearchStatusException(
                        "Could not complete inference endpoint creation as validation call to service returned null response.",
                        RestStatus.BAD_REQUEST
                    )
                );
            }
        }, e -> {
            listener.onFailure(
                new ElasticsearchStatusException(
                    "Could not complete inference endpoint creation as validation call to service threw an exception.",
                    RestStatus.BAD_REQUEST,
                    e
                )
            );
        });
    }
}
