/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.aggregation;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.xpack.ml.datafeed.LinkedClusterState;

import java.util.List;

/**
 * Thrown by aggregation data extractors when a CCS search skips one or more remote clusters.
 * Carries the per-cluster states extracted from the search response before it was released,
 * so that callers can propagate them to
 * {@link org.elasticsearch.xpack.ml.datafeed.CrossClusterSearchStats} even when extraction fails.
 */
class SkippedClustersException extends RuntimeException {

    private final ResourceNotFoundException resourceNotFoundException;
    private final List<LinkedClusterState> linkedClusterStates;

    SkippedClustersException(ResourceNotFoundException cause, List<LinkedClusterState> linkedClusterStates) {
        super(cause.getMessage(), cause);
        this.resourceNotFoundException = cause;
        this.linkedClusterStates = List.copyOf(linkedClusterStates);
    }

    ResourceNotFoundException getResourceNotFoundException() {
        return resourceNotFoundException;
    }

    List<LinkedClusterState> getLinkedClusterStates() {
        return linkedClusterStates;
    }
}
