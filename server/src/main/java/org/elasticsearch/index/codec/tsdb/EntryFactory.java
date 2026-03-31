/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer.BinaryEntry;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer.NumericEntry;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer.SortedEntry;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer.SortedNumericEntry;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer.SortedSetEntry;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer.TermsDictEntry;

/**
 * Factory for creating entry instances during segment metadata reading.
 * Each codec version provides its own factory to return codec-specific
 * subclasses of the base entry types. All methods have default implementations
 * returning base types; codecs override only the entries they need.
 */
public interface EntryFactory {

    /**
     * Creates a numeric entry.
     *
     * @return the numeric entry
     */
    default NumericEntry createNumericEntry() {
        return new NumericEntry();
    }

    /**
     * Creates a sorted-numeric entry.
     *
     * @return the sorted-numeric entry
     */
    default SortedNumericEntry createSortedNumericEntry() {
        return new SortedNumericEntry();
    }

    /**
     * Creates a sorted entry.
     *
     * @return the sorted entry
     */
    default SortedEntry createSortedEntry() {
        return new SortedEntry();
    }

    /**
     * Creates a sorted-set entry.
     *
     * @return the sorted-set entry
     */
    default SortedSetEntry createSortedSetEntry() {
        return new SortedSetEntry();
    }

    /**
     * Creates a terms dictionary entry.
     *
     * @return the terms dictionary entry
     */
    default TermsDictEntry createTermsDictEntry() {
        return new TermsDictEntry();
    }

    /**
     * Creates a binary entry.
     *
     * @return the binary entry
     */
    default BinaryEntry createBinaryEntry() {
        return new BinaryEntry();
    }
}
