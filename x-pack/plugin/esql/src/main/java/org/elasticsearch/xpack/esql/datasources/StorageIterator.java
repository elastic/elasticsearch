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
 * Iterator over objects in a directory. Must be closed to release resources.
 * Supports lazy loading for large directories.
 */
public interface StorageIterator extends Iterator<StorageEntry>, Closeable {}
