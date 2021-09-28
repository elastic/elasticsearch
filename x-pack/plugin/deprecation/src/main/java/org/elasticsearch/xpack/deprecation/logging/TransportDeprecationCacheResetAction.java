/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation.logging;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.RateLimitingFilter;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

public class TransportDeprecationCacheResetAction
    extends HandledTransportAction<DeprecationCacheResetAction.Request, ActionResponse.Empty> {
    private static final DeprecationLogger logger = DeprecationLogger.getLogger(TransportDeprecationCacheResetAction.class);

    private final RateLimitingFilter rateLimitingFilterForIndexing;

    @Inject
    public TransportDeprecationCacheResetAction( TransportService transportService, ActionFilters actionFilters,
                                                 RateLimitingFilter rateLimitingFilterForIndexing) {
        super(DeprecationCacheResetAction.NAME, transportService, actionFilters, DeprecationCacheResetAction.Request::new);
        this.rateLimitingFilterForIndexing = rateLimitingFilterForIndexing;
    }

    @Override
    protected void doExecute(Task task, DeprecationCacheResetAction.Request request, ActionListener<ActionResponse.Empty> listener) {
        rateLimitingFilterForIndexing.reset();
        logger.warn(DeprecationCategory.CACHE_RESET,"cache_reset", "Deprecation cache was reset");
        listener.onResponse(ActionResponse.Empty.INSTANCE);
    }
}
