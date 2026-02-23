/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.esql.datasources.CloseableIterator;

/**
 * Streaming cursor over query results from a connector.
 * Each call to {@link #next()} returns a {@link Page} of columnar data.
 */
public interface ResultCursor extends CloseableIterator<Page> {

    default void cancel() {}
}
