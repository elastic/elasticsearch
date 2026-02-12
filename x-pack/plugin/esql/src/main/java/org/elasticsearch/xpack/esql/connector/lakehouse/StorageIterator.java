/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.connector.lakehouse;

/**
 * Iterator over objects in a storage directory. Must be closed to release resources.
 * Supports lazy loading for large directories.
 *
 * <p>Extends {@link CloseableIterator} with a storage-specific type to allow
 * future additions (e.g., pagination tokens, estimated remaining count).
 */
public interface StorageIterator extends CloseableIterator<StorageEntry> {}
