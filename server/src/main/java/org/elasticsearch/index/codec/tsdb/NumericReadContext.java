/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

/**
 * Shared read-path state for numeric field readers. Created once per segment
 * by the producer and passed to {@link NumericBlockCodec#createReader}.
 *
 * @param blockSize    the number of values per numeric block
 * @param formatConfig the format configuration for this codec version
 */
public record NumericReadContext(int blockSize, TSDBDocValuesFormatConfig formatConfig) {}
