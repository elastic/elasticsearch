/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * Payload encoder/decoder contracts for numeric pipeline stages.
 *
 * <p>{@link org.elasticsearch.index.codec.tsdb.pipeline.numeric.PayloadEncoder} and
 * {@link org.elasticsearch.index.codec.tsdb.pipeline.numeric.PayloadDecoder} define
 * the terminal stage contract: serialize transformed {@code long[]} values to bytes
 * and read them back. Each implementation corresponds to a
 * {@link org.elasticsearch.index.codec.tsdb.pipeline.StageSpec.PayloadSpec} and is
 * identified by a {@link org.elasticsearch.index.codec.tsdb.pipeline.StageId} for
 * wire-format lookup during decoding.
 */
package org.elasticsearch.index.codec.tsdb.pipeline.numeric;
