/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.sort;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.search.DocValueFormat;

public class BucketedSortForLongsTests extends BucketedSortTestCase<BucketedSort.ForLongs> {
    @Override
    public BucketedSort.ForLongs build(
        SortOrder sortOrder,
        DocValueFormat format,
        int bucketSize,
        BucketedSort.ExtraData extra,
        double[] values
    ) {
        return new BucketedSort.ForLongs(bigArrays(), sortOrder, format, bucketSize, extra) {
            @Override
            public Leaf forLeaf(LeafReaderContext ctx) {
                return new Leaf(ctx) {
                    int index = -1;

                    @Override
                    protected boolean advanceExact(int doc) {
                        index = doc;
                        return doc < values.length;
                    }

                    @Override
                    protected long docValue() {
                        return (long) values[index];
                    }
                };
            }
        };
    }

    @Override
    protected SortValue expectedSortValue(double v) {
        return SortValue.from((long) v);
    }

    @Override
    protected double randomValue() {
        // 2L^50 fits in the mantisa of a double which the test sort of needs.
        return randomLongBetween(-(1L << 50), (1L << 50));
    }
}
