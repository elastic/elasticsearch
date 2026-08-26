/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric;

/**
 * A combined encoder/decoder for a non-terminal transform stage.
 *
 * <p>Implementations are stateless singletons that transform {@code long[]} values
 * in-place. Each stage reduces the dynamic range of the values (e.g., by computing
 * deltas, removing offsets, or factoring out a GCD) so that the terminal payload
 * stage can pack them into fewer bits.
 */
public interface NumericCodecStage extends TransformEncoder, TransformDecoder {}
