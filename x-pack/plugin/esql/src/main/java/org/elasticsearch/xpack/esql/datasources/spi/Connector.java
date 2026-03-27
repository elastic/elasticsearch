/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.io.Closeable;
import java.util.List;

/**
 * A live connection to an external data source.
 * Opened by {@link ConnectorFactory#open} and closed after query execution.
 */
public interface Connector extends Closeable {

    default List<Split> discoverSplits(QueryRequest request) {
        return List.of(Split.SINGLE);
    }

    ResultCursor execute(QueryRequest request, Split split);
}
