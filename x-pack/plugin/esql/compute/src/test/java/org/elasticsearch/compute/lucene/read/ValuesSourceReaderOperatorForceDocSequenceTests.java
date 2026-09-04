/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.apache.lucene.tests.util.LuceneTestCase;

/**
 * Runs all {@link ValuesSourceReaderOperatorTests} with the doc-sequence threshold
 * set to {@code 0}, forcing {@link ValuesFromDocSequence} for every multi-segment page
 * regardless of the number of {@code BytesRef} fields. This validates the correctness
 * of the doc-sequence loading path across all existing test scenarios.
 */
@LuceneTestCase.SuppressFileSystems(value = "HandleLimitFS")
public class ValuesSourceReaderOperatorForceDocSequenceTests extends ValuesSourceReaderOperatorTests {

    @Override
    protected int docSequenceBytesRefFieldThreshold() {
        return 0;
    }
}
