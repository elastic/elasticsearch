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

/**
 * A {@link DocValuesProducer} that carries {@link DocValuesConsumerUtil.MergeStats} alongside the doc values data.
 * <p>
 * On the merge path, anonymous subclasses override the doc values methods they need; non-overridden methods
 * throw {@link UnsupportedOperationException}. On the add/indexing path, {@link IndexingDocValuesSource} wraps
 * a real {@link DocValuesProducer} and reports stats as {@link DocValuesConsumerUtil#UNSUPPORTED}.
 * <p>
 * Use {@link #fromProducer(DocValuesProducer)} to normalize any {@link DocValuesProducer} into a
 * {@link DocValuesSource}, ensuring consumer code can access {@link #mergeStats} uniformly without
 * {@code instanceof} checks.
 */
public abstract class DocValuesSource extends EmptyDocValuesProducer {

    public final DocValuesConsumerUtil.MergeStats mergeStats;

    /**
     * Creates a new doc values source with the given merge stats.
     *
     * @param mergeStats pre-computed merge stats, or {@link DocValuesConsumerUtil#UNSUPPORTED} if stats are not available
     */
    protected DocValuesSource(final DocValuesConsumerUtil.MergeStats mergeStats) {
        this.mergeStats = mergeStats;
    }

    /**
     * Normalizes a {@link DocValuesProducer} into a {@link DocValuesSource}.
     * If the producer is already a {@link DocValuesSource}, it is returned as-is.
     * Otherwise, it is wrapped in an {@link IndexingDocValuesSource}.
     *
     * @param producer the producer to normalize
     * @return the producer as a {@link DocValuesSource}
     */
    public static DocValuesSource fromProducer(final DocValuesProducer producer) {
        return producer instanceof DocValuesSource source ? source : new IndexingDocValuesSource(producer);
    }
}
