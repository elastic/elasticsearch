/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import java.io.Closeable;
import java.util.Iterator;

/**
 * Iterator that must be closed to release resources.
 *
 * @param <T> the type of elements returned by this iterator
 */
public interface CloseableIterator<T> extends Iterator<T>, Closeable {}
