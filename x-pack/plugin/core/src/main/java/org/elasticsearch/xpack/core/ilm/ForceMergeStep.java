/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;

import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Invokes a force merge on a single index.
 */
public class ForceMergeStep extends AsyncActionStep {

    public static final String NAME = "forcemerge";
    private static final Logger logger = LogManager.getLogger(ForceMergeStep.class);
    private final int maxNumSegments;

    public ForceMergeStep(StepKey key, StepKey nextStepKey, Client client, int maxNumSegments) {
        super(key, nextStepKey, client);
        this.maxNumSegments = maxNumSegments;
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    public int getMaxNumSegments() {
        return maxNumSegments;
    }

    @Override
    public void performAction(
        IndexMetadata indexMetadata,
        ClusterState currentState,
        ClusterStateObserver observer,
        ActionListener<Void> listener
    ) {
        String indexName = indexMetadata.getIndex().getName();
        ForceMergeRequest request = new ForceMergeRequest(indexName);
        request.maxNumSegments(maxNumSegments);
        getClient().admin().indices().forceMerge(request, listener.wrapResponse((l, response) -> {
            if (response.getFailedShards() == 0) {
                l.onResponse(null);
            } else {
                DefaultShardOperationFailedException[] failures = response.getShardFailures();
                String policyName = indexMetadata.getLifecyclePolicyName();
                String errorMessage = String.format(
                    Locale.ROOT,
                    "index [%s] in policy [%s] encountered failures [%s] on step [%s]",
                    indexName,
                    policyName,
                    failures == null
                        ? "n/a"
                        : Strings.collectionToDelimitedString(
                            Arrays.stream(failures).map(Strings::toString).collect(Collectors.toList()),
                            ","
                        ),
                    NAME
                );
                logger.warn(errorMessage);
                // let's report it as a failure and retry
                l.onFailure(new ElasticsearchException(errorMessage));
            }
        }));
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), maxNumSegments);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ForceMergeStep other = (ForceMergeStep) obj;
        return super.equals(obj) && Objects.equals(maxNumSegments, other.maxNumSegments);
    }
}
