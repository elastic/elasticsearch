/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.mapreduce;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSource.Bytes;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Interface to extract values from Lucene in order to feed it into the MapReducer.
 */
public abstract class ValuesExtractor {

    protected final String fieldName;
    protected final DocValueFormat format;

    abstract Tuple<String, List<Object>> collectValues(LeafReaderContext ctx, int doc) throws IOException;

    ValuesExtractor(String fieldName, DocValueFormat format) {
        this.fieldName = fieldName;
        this.format = format;
    }

    static ValuesExtractor buildBytesExtractor(ValuesSourceConfig config) {
        ValuesSource vs = config.getValuesSource();
        String fieldName = config.fieldContext() != null ? config.fieldContext().field() : null;

        return new Keyword(fieldName, (Bytes) vs, config.format());
    }

    static ValuesExtractor buildLongExtractor(ValuesSourceConfig config) {
        ValuesSource vs = config.getValuesSource();
        String fieldName = config.fieldContext() != null ? config.fieldContext().field() : null;

        if (Strings.isNullOrEmpty(fieldName)) {
            throw new IllegalArgumentException("scripts are not supported");
        }
        return new LongValue(fieldName, (Numeric) vs, config.format());
    }

    public static class Keyword extends ValuesExtractor {
        private final ValuesSource.Bytes source;

        Keyword(String fieldName, ValuesSource.Bytes source, DocValueFormat format) {
            super(fieldName, format);
            this.source = source;
        }

        @Override
        public Tuple<String, List<Object>> collectValues(LeafReaderContext ctx, int doc) throws IOException {

            SortedBinaryDocValues values = source.bytesValues(ctx);

            if (values.advanceExact(doc)) {
                int valuesCount = values.docValueCount();
                List<Object> objects = new ArrayList<>(valuesCount);

                for (int i = 0; i < valuesCount; ++i) {
                    objects.add(format.format(values.nextValue()));
                }
                return new Tuple<>(fieldName, objects);
            }
            return new Tuple<>(fieldName, Collections.emptyList());
        }
    }

    public static class LongValue extends ValuesExtractor {
        private final ValuesSource.Numeric source;

        LongValue(String fieldName, ValuesSource.Numeric source, DocValueFormat format) {
            super(fieldName, format);
            this.source = source;
        }

        @Override
        public Tuple<String, List<Object>> collectValues(LeafReaderContext ctx, int doc) throws IOException {

            SortedNumericDocValues values = source.longValues(ctx);

            if (values.advanceExact(doc)) {
                int valuesCount = values.docValueCount();
                List<Object> objects = new ArrayList<>(valuesCount);

                for (int i = 0; i < valuesCount; ++i) {
                    objects.add(format.format(values.nextValue()));
                }
                return new Tuple<>(fieldName, objects);
            }
            return new Tuple<>(fieldName, Collections.emptyList());
        }

    }
}
