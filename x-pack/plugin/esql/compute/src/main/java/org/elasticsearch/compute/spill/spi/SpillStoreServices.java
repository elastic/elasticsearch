/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill.spi;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Executor;

/**
 * What a backend may use, and nothing from the engine.
 *
 * @param settings the node settings, for the backend's own static settings
 * @param clusterSettings for the backend's dynamic settings, watched through the usual consumers
 * @param dataPaths the node's data paths, in order; a backend that stores under them uses the first
 * @param nodeId the node id, so that two nodes on one host keep apart
 * @param ioExecutor the spill I/O pool, with a bounded queue, where every blocking operation of the backend runs
 */
public record SpillStoreServices(
    Settings settings,
    ClusterSettings clusterSettings,
    List<Path> dataPaths,
    String nodeId,
    Executor ioExecutor
) {}
