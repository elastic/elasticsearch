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
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
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
public abstract class MapReduceValueSource {

    @FunctionalInterface
    public interface ValueSourceSupplier {
        MapReduceValueSource build(ValuesSourceConfig config, int id);
    }

    public static class Field implements Writeable {
        private final String name;
        private final int id;

        public Field(String name, int id) {
            this.name = name;
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public int getId() {
            return id;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeInt(id);

        }
    };

    private final Field field;
    private final DocValueFormat format;

    abstract Tuple<Field, List<Object>> collect(LeafReaderContext ctx, int doc) throws IOException;

    MapReduceValueSource(ValuesSourceConfig config, int id) {
        String fieldName = config.fieldContext() != null ? config.fieldContext().field() : null;

        if (Strings.isNullOrEmpty(fieldName)) {
            throw new IllegalArgumentException("scripts are not supported");
        }

        this.field = new Field(fieldName, id);
        this.format = config.format();
    }

    public Field getField() {
        return field;
    }

    DocValueFormat getFormat() {
        return format;
    }

    public static class KeywordValueSource extends MapReduceValueSource {
        private final ValuesSource.Bytes source;

        public KeywordValueSource(ValuesSourceConfig config, int id) {
            super(config, id);
            this.source = (Bytes) config.getValuesSource();
        }

        @Override
        public Tuple<Field, List<Object>> collect(LeafReaderContext ctx, int doc) throws IOException {
            SortedBinaryDocValues values = source.bytesValues(ctx);

            if (values.advanceExact(doc)) {
                int valuesCount = values.docValueCount();
                List<Object> objects = new ArrayList<>(valuesCount);

                for (int i = 0; i < valuesCount; ++i) {
                    objects.add(getFormat().format(values.nextValue()));
                }
                return new Tuple<>(getField(), objects);
            }
            return new Tuple<>(getField(), Collections.emptyList());
        }
    }

    public static class NumericValueSource extends MapReduceValueSource {
        private final ValuesSource.Numeric source;

        public NumericValueSource(ValuesSourceConfig config, int id) {
            super(config, id);
            this.source = (Numeric) config.getValuesSource();
        }

        @Override
        public Tuple<Field, List<Object>> collect(LeafReaderContext ctx, int doc) throws IOException {
            SortedNumericDocValues values = source.longValues(ctx);

            if (values.advanceExact(doc)) {
                int valuesCount = values.docValueCount();
                List<Object> objects = new ArrayList<>(valuesCount);

                for (int i = 0; i < valuesCount; ++i) {
                    objects.add(getFormat().format(values.nextValue()));
                }
                return new Tuple<>(getField(), objects);
            }
            return new Tuple<>(getField(), Collections.emptyList());
        }

    }
}
