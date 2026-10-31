/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.EmptyDocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;

import java.io.IOException;

/**
 * Wraps a {@link DocValuesProducer} to carry pre-computed {@link DocValuesConsumerUtil.MergeStats}
 * through the optimized merge path. When wrapping another {@code TsdbDocValuesProducer}, the merge
 * stats are carried over to avoid recomputation.
 */
public class TsdbDocValuesProducer extends EmptyDocValuesProducer {

    public final DocValuesConsumerUtil.MergeStats mergeStats;
    private final DocValuesProducer actual;
    /** Set at flush for a single-valued numeric field, see {@link MaterializedNumericValues}. */
    private MaterializedNumericValues materialized;

    public TsdbDocValuesProducer(DocValuesConsumerUtil.MergeStats mergeStats) {
        this.mergeStats = mergeStats;
        this.actual = null;
    }

    public TsdbDocValuesProducer(DocValuesProducer valuesProducer) {
        if (valuesProducer instanceof TsdbDocValuesProducer tsdb) {
            mergeStats = tsdb.mergeStats;
        } else {
            mergeStats = DocValuesConsumerUtil.UNSUPPORTED;
        }
        this.actual = valuesProducer;
    }

    /**
     * The field's values already read into arrays, or {@code null} when the writer has to walk {@link #getSortedNumeric} instead.
     * When present, it holds exactly the values {@link #getSortedNumeric} would return, and the field is single-valued.
     */
    MaterializedNumericValues materialized() {
        return materialized;
    }

    void materialized(MaterializedNumericValues materialized) {
        this.materialized = materialized;
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) throws IOException {
        if (actual != null) {
            return actual.getSorted(field);
        } else {
            return super.getSorted(field);
        }
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
        if (actual != null) {
            return actual.getSortedSet(field);
        } else {
            return super.getSortedSet(field);
        }
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        if (actual != null) {
            return actual.getSortedNumeric(field);
        } else {
            return super.getSortedNumeric(field);
        }
    }
}
