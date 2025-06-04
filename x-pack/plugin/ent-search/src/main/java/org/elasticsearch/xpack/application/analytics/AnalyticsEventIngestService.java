/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.xpack.application.analytics.action.PostAnalyticsEventAction;
import org.elasticsearch.xpack.application.analytics.ingest.AnalyticsEventEmitter;

import java.util.Objects;

/**
 * Event emitter will index Analytics events submitted through a @{PostAnalyticsEventAction.Request} request.
 * @deprecated in 9.0
 */
@Deprecated
@UpdateForV10(owner = UpdateForV10.Owner.ENTERPRISE_SEARCH)
public class AnalyticsEventIngestService {
    private final AnalyticsCollectionResolver collectionResolver;

    private final AnalyticsEventEmitter eventEmitter;

    @Inject
    public AnalyticsEventIngestService(AnalyticsEventEmitter eventEmitter, AnalyticsCollectionResolver collectionResolver) {
        this.eventEmitter = Objects.requireNonNull(eventEmitter, "eventEmitter cannot be null");
        this.collectionResolver = Objects.requireNonNull(collectionResolver, "collectionResolver cannot be null");
    }

    /**
     * Emit an analytics event from a request.
     * If the collection does not exist, the listener will receive a {@link ResourceNotFoundException}.
     *
     * @param request the request containing the analytics event data
     * @param listener the listener to call once the event has been emitted
     */
    public void addEvent(PostAnalyticsEventAction.Request request, ActionListener<PostAnalyticsEventAction.Response> listener) {
        try {
            collectionResolver.collection(request.eventCollectionName());
        } catch (ResourceNotFoundException e) {
            listener.onFailure(e);
            return;
        }

        eventEmitter.emitEvent(request, listener);
    }
}
