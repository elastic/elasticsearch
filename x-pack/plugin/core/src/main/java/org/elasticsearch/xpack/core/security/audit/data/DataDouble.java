/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.audit.data;

/**
 * An immutable JSON decimal value that is exactly representable as a 64-bit {@code double}: the common subset of JSON
 * and the OTel attribute model. Decimals not exactly representable as a {@code double} are held faithfully as a
 * {@link DataDecimal} instead (see {@link DataValue#of(java.math.BigDecimal)}).
 */
public record DataDouble(double value) implements DataValue {}
