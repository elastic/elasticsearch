/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.aggregations.support;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.Rounding.Prepared;
import org.elasticsearch.index.fielddata.DocValueBits;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.xpack.analytics.mapper.HyperLogLogPlusPlusFieldMapper;
import org.elasticsearch.xpack.analytics.mapper.fielddata.HyperLogLogPlusPlusValues;
import org.elasticsearch.xpack.analytics.mapper.fielddata.IndexHyperLogLogPlusPlusFieldData;

import java.io.IOException;
import java.util.function.Function;

public class HyperLogLogPlusPlusValuesSource {
    public abstract static class HyperLogLogPlusPlusSketch extends ValuesSource {

        public abstract HyperLogLogPlusPlusValues getHyperLogLogPlusPlusValues(LeafReaderContext context) throws IOException;

        @Override
        public Function<Rounding, Prepared> roundingPreparer() {
            throw new AggregationExecutionException("can't round a [" + HyperLogLogPlusPlusFieldMapper.CONTENT_TYPE + "]");
        }

        public static class Fielddata extends HyperLogLogPlusPlusSketch {

            protected final IndexHyperLogLogPlusPlusFieldData indexFieldData;

            public Fielddata(IndexHyperLogLogPlusPlusFieldData indexFieldData) {
                this.indexFieldData = indexFieldData;
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) {
                return indexFieldData.load(context).getBytesValues();
            }

            @Override
            public DocValueBits docsWithValue(LeafReaderContext context) throws IOException {
                HyperLogLogPlusPlusValues values = getHyperLogLogPlusPlusValues(context);
                return new DocValueBits() {
                    @Override
                    public boolean advanceExact(int doc) throws IOException {
                        return values.advanceExact(doc);
                    }
                };
            }

            @Override
            public HyperLogLogPlusPlusValues getHyperLogLogPlusPlusValues(LeafReaderContext context) throws IOException {
                return indexFieldData.load(context).getHllValues();
            }
        }
    }
}
