/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.index.Index;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.function.LongSupplier;

/**
 * Executes an action on an index related to its lifecycle.
 */
public interface LifecycleAction extends ToXContentObject, NamedWriteable {

    List<Step> toSteps(String phase);

    default boolean indexSurvives() {
        return true;
    }
}
