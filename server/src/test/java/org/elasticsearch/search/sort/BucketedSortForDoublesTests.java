/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.sort;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.search.DocValueFormat;

public class BucketedSortForDoublesTests extends BucketedSortTestCase<BucketedSort.ForDoubles> {
    @Override
    public BucketedSort.ForDoubles build(SortOrder sortOrder, DocValueFormat format, int bucketSize,
            BucketedSort.ExtraData extra, double[] values) {
        return new BucketedSort.ForDoubles(bigArrays(), sortOrder, format, bucketSize, extra) {
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
                    protected double docValue() {
                        return values[index];
                    }
                };
            }
        };
    }

    @Override
    protected SortValue expectedSortValue(double v) {
        return SortValue.from(v);
    }

    @Override
    protected double randomValue() {
        return randomDouble();
    }
}
