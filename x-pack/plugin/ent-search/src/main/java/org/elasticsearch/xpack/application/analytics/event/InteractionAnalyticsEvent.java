/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.application.analytics.AnalyticsCollection;

import java.io.IOException;

public class InteractionAnalyticsEvent extends AnalyticsEvent {
    public InteractionAnalyticsEvent(AnalyticsCollection analyticsCollection, SessionData sessionData, UserData userData) {
        super(analyticsCollection, sessionData, userData);
    }

    public InteractionAnalyticsEvent(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected Type getType() {
        return Type.INTERACTION;
    }
}
