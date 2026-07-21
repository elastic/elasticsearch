/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datatree;

/**
 * An immutable JSON integer value that fits in a signed 64-bit {@code long}: the common subset of JSON and the OTel
 * attribute model. Integers outside {@code long} range are held faithfully as a {@link DataInteger} instead (see
 * {@link DataValue#of(java.math.BigInteger)}).
 */
public record DataLong(long value) implements DataValue {}
