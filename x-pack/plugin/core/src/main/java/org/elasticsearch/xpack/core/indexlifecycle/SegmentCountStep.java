/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.Index;

import java.util.Arrays;
import java.util.stream.StreamSupport;

public class SegmentCountStep extends AsyncWaitStep {

    private final int maxNumSegments;

    public SegmentCountStep(StepKey key, StepKey nextStepKey, Client client, int maxNumSegments) {
        super(key, nextStepKey, client);
        this.maxNumSegments = maxNumSegments;
    }

    @Override
    public void evaluateCondition(Index index, Listener listener) {
        getClient().admin().indices().prepareSegments(index.getName()).execute(ActionListener.wrap(response -> {
            listener.onResponse(StreamSupport.stream(response.getIndices().get(index.getName()).spliterator(), false)
                .anyMatch(iss -> Arrays.stream(iss.getShards()).anyMatch(p -> p.getSegments().size() > maxNumSegments)));
        }, listener::onFailure));
    }
}
