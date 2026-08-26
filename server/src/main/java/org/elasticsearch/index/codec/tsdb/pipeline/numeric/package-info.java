/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * Encoder/decoder contracts and pipeline orchestration for numeric pipeline stages.
 *
 * <p>This package defines two families of stage contracts:
 * <ul>
 *   <li><strong>Transform stages</strong>:
 *       {@link org.elasticsearch.index.codec.tsdb.pipeline.numeric.TransformEncoder} and
 *       {@link org.elasticsearch.index.codec.tsdb.pipeline.numeric.TransformDecoder}
 *       (combined as {@link org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodecStage})
 *       modify {@code long[]} values in-place and exchange metadata through the
 *       encoding/decoding context. Concrete implementations live in the
 *       {@link org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages} subpackage.</li>
 *   <li><strong>Payload stages</strong>:
 *       {@link org.elasticsearch.index.codec.tsdb.pipeline.numeric.PayloadEncoder} and
 *       {@link org.elasticsearch.index.codec.tsdb.pipeline.numeric.PayloadDecoder}
 *       (combined as {@link org.elasticsearch.index.codec.tsdb.pipeline.numeric.PayloadCodecStage})
 *       serialize transformed values to bytes and read them back as the terminal
 *       pipeline step.</li>
 * </ul>
 *
 * <p>Pipeline orchestration is provided by {@link org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericEncodePipeline}
 * (write path) and {@link org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericDecodePipeline} (read path).
 * The high-level API is exposed via {@link org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericEncoder}
 * and {@link org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericDecoder}, which produce per-field
 * {@link org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericBlockEncoder} and
 * {@link org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericBlockDecoder} instances owning
 * mutable per-block context.
 *
 * <p>Each stage implementation corresponds to a
 * {@link org.elasticsearch.index.codec.tsdb.pipeline.StageSpec} and is identified by a
 * {@link org.elasticsearch.index.codec.tsdb.pipeline.StageId} for wire-format lookup
 * during decoding.
 */
package org.elasticsearch.index.codec.tsdb.pipeline.numeric;
