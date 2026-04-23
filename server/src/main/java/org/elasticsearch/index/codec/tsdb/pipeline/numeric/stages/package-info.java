/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * Concrete implementations of numeric pipeline stages.
 *
 * <p>This package contains the four stages that form the standard numeric compression
 * pipeline:
 * <ul>
 *   <li>{@link org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.DeltaCodecStage}
 *       - delta encoding for monotonic sequences</li>
 *   <li>{@link org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.OffsetCodecStage}
 *       - offset removal when the minimum is significant</li>
 *   <li>{@link org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.GcdCodecStage}
 *       - GCD factoring when values share a common divisor</li>
 *   <li>{@link org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.BitPackCodecStage}
 *       - bit-packing terminal payload stage</li>
 * </ul>
 *
 * <p>The first three are transform stages (stateless singletons implementing
 * {@link org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodecStage}) that
 * reduce the dynamic range of values in-place. The last is the terminal payload stage
 * (implementing {@link org.elasticsearch.index.codec.tsdb.pipeline.numeric.PayloadCodecStage})
 * that serializes the reduced values to a compact binary representation.
 */
package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;
