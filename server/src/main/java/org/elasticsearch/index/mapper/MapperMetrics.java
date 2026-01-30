/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

/**
 * Groups together all metrics used in mappers.
 * Main purpose of this class is to avoid verbosity of passing individual metric instances around.
 */
public record MapperMetrics(SourceFieldMetrics sourceFieldMetrics) {
    public static final MapperMetrics NOOP = new MapperMetrics(SourceFieldMetrics.NOOP);
}
