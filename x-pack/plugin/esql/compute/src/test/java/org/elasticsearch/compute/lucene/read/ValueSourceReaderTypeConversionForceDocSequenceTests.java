/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

/**
 * Runs all {@link ValueSourceReaderTypeConversionTests} with the doc-sequence threshold
 * set to {@code 0}, forcing {@link ValuesFromDocSequence} for every multi-segment page
 * regardless of the number of {@code BytesRef} fields.
 */
public class ValueSourceReaderTypeConversionForceDocSequenceTests extends ValueSourceReaderTypeConversionTests {

    @Override
    protected int docSequenceBytesRefFieldThreshold() {
        return 0;
    }
}
