/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import java.io.Closeable;
import java.util.Iterator;

/**
 * An {@link Iterator} with state that must be {@link #close() closed}.
 */
public interface CloseableIterator<T> extends Iterator<T>, Closeable {}
