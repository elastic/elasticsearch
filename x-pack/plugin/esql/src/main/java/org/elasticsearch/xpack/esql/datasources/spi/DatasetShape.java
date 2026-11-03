/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.core.Nullable;

/**
 * Pre-clamp format and compression tokens derived from a stored dataset. Null means
 * "could not resolve"; the inventory layer maps that to {@code unresolved} / {@code unknown}.
 * {@code auto} is never a format token.
 */
public record DatasetShape(@Nullable String format, @Nullable String compression) {}
