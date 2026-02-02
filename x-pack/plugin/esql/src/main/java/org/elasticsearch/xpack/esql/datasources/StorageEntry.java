/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.time.Instant;

/**
 * Metadata about an object returned from directory listing.
 */
public record StorageEntry(StoragePath path, long length, Instant lastModified) {}
