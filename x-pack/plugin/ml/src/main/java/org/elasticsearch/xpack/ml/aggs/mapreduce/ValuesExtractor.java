/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.mapreduce;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Interface to extract values from Lucene in order to feed it into the MapReducer.
 */
public abstract class ValuesExtractor {
    protected final ValuesSource source;
    protected final String fieldName;

    abstract Tuple<String, List<Object>> collectValues(LeafReaderContext ctx, int doc) throws IOException;

    ValuesExtractor(String fieldName, ValuesSource source) {
        this.fieldName = fieldName;
        this.source = source;
    }

    public static class Keyword extends ValuesExtractor {

        Keyword(String fieldName, ValuesSource source) {
            super(fieldName, source);
        }

        @Override
        public Tuple<String, List<Object>> collectValues(LeafReaderContext ctx, int doc) throws IOException {

            SortedBinaryDocValues values = source.bytesValues(ctx);

            if (values.advanceExact(doc)) {
                int valuesCount = values.docValueCount();
                List<Object> objects = new ArrayList<>(valuesCount);

                for (int i = 0; i < valuesCount; ++i) {
                    objects.add(values.nextValue().utf8ToString());
                }
                return new Tuple<>(fieldName, objects);
            }
            return new Tuple<>(fieldName, Collections.emptyList());
        }

    }
}
