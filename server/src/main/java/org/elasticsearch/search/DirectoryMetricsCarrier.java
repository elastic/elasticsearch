/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.index.store.DirectoryMetrics;

/**
 * Implemented by {@link SearchPhaseResult} subclasses that carry directory-level metrics
 * captured during shard-level execution.
 */
public interface DirectoryMetricsCarrier {

    /**
     * Returns the directory-level metrics captured during this phase execution on a data node.
     * Should never return null, but {@link DirectoryMetrics#EMPTY} in case no metrics were
     * captured.
     */
    DirectoryMetrics getDirectoryMetrics();

    void setDirectoryMetrics(DirectoryMetrics directoryMetrics);
}
