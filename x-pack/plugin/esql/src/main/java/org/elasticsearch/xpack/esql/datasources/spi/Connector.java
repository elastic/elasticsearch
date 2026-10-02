/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.io.Closeable;

/**
 * A live connection to an external data source.
 * Opened by {@link ConnectorFactory#open} and closed after query execution.
 *
 * Split discovery is handled by {@link SplitProvider} (via the factory), not
 * by the connector itself. The framework passes each discovered
 * {@link ExternalSplit} to {@link #execute(QueryRequest, ExternalSplit)}.
 * Connectors that do not support parallel reads receive
 * {@link Split#SINGLE} through the legacy overload.
 */
public interface Connector extends Closeable {

    ResultCursor execute(QueryRequest request, Split split);

    default ResultCursor execute(QueryRequest request, ExternalSplit split) {
        return execute(request, Split.SINGLE);
    }
}
