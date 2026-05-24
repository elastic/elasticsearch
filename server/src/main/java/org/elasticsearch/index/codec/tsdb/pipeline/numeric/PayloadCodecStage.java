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
 * A combined encoder/decoder for a terminal payload stage.
 *
 * <p>Implementations serialize transformed {@code long[]} values to bytes on encode
 * and deserialize them back on decode. This is always the last stage in the pipeline.
 */
public interface PayloadCodecStage extends PayloadEncoder, PayloadDecoder {}
