/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.xcontent.ToXContentObject;

/**
 * A step which will be called periodically, waiting for some condition to become true.
 * Called asynchronously, as the condition may take time to check.
 * <p>
 * If checking something based on the current cluster state which does not take time to check, use {@link ClusterStateWaitStep}.
 */
public abstract class AsyncWaitStep extends Step {

    @Nullable
    private final Client client;

    public AsyncWaitStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey);
        this.client = client;
    }

    @Nullable
    protected Client getClient() {
        return client;
    }

    public abstract void evaluateCondition(Metadata metadata, Index index, Listener listener, TimeValue masterTimeout);

    public interface Listener {

        void onResponse(boolean conditionMet, ToXContentObject informationContext);

        void onFailure(Exception e);
    }
}
