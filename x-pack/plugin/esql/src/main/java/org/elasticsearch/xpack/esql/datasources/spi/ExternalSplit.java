/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.common.io.stream.NamedWriteable;

/**
 * A serializable, parallelizable unit of work for an external data source.
 * Each split represents a portion of data (e.g., a file slice, a partition)
 * that can be read independently by a single driver and shipped across nodes.
 */
public interface ExternalSplit extends NamedWriteable {

    String sourceType();

    default long estimatedSizeInBytes() {
        return -1;
    }
}
