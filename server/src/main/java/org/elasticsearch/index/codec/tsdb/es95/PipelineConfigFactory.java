/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;

/**
 * Factory that creates a {@link PipelineConfig} for a given block size.
 * The format class provides the implementation; the numeric codec and
 * field writer consume it.
 */
@FunctionalInterface
interface PipelineConfigFactory {

    /**
     * Creates a pipeline configuration for the given block size.
     *
     * @param blockSize the number of values per numeric block
     * @return the pipeline configuration
     */
    PipelineConfig create(int blockSize);
}
