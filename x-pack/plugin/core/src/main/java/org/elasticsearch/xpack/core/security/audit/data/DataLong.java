/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.audit.data;

/**
 * An immutable JSON integer value that fits in a signed 64-bit {@code long}: the common subset of JSON and the OTel
 * attribute model. Integers outside {@code long} range are held faithfully as a {@link DataInteger} instead (see
 * {@link DataValue#of(java.math.BigInteger)}).
 */
public record DataLong(long value) implements DataValue {}
