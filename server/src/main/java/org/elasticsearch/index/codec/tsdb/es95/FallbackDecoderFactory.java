/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.elasticsearch.index.codec.tsdb.NumericFieldReader;

/**
 * Factory that creates a non-pipeline {@link NumericFieldReader.Decoder} for a given block size.
 * Used by {@link ES95NumericFieldReader} when the pipeline was not applied to a field,
 * for example ordinal-range and single-ordinal encoded fields, or segments written by
 * older codec versions that did not use pipeline encoding.
 */
@FunctionalInterface
interface FallbackDecoderFactory {

    /**
     * Creates a fallback decoder for the given block size.
     *
     * @param blockSize the number of values per numeric block
     * @return the fallback decoder
     */
    NumericFieldReader.Decoder create(int blockSize);
}
