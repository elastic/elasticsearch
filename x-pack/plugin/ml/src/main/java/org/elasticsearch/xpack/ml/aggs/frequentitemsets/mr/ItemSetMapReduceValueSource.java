/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets.mr;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
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
import java.util.Objects;

/**
 * Interface to extract values from Lucene in order to feed it into the MapReducer.
 */
public abstract class ItemSetMapReduceValueSource {

    @FunctionalInterface
    public interface ValueSourceSupplier {
        ItemSetMapReduceValueSource build(ValuesSourceConfig config, int id);
    }

    enum ValueFormatter {
        BYTES_REF {
            @Override
            public Object format(DocValueFormat format, Object obj) {
                return format.format((BytesRef) obj);
            }
        },
        LONG {
            @Override
            public Object format(DocValueFormat format, Object obj) {
                return format.format((Long) obj);
            }
        };

        Object format(DocValueFormat format, Object obj) {
            throw new UnsupportedOperationException();
        }
    }

    public static class Field implements Writeable {
        private final String name;
        private final int id;
        private final DocValueFormat format;
        private final ValueFormatter valueFormatter;

        Field(String name, int id, DocValueFormat format, ValueFormatter valueFormatter) {
            this.name = name;
            this.id = id;
            this.format = format;
            this.valueFormatter = valueFormatter;
        }

        Field(StreamInput in) throws IOException {
            this.name = in.readString();
            this.id = in.readVInt();
            this.format = in.readNamedWriteable(DocValueFormat.class);
            this.valueFormatter = in.readEnum(ValueFormatter.class);
        }

        public String getName() {
            return name;
        }

        public int getId() {
            return id;
        }

        public Object formatValue(Object value) {
            return valueFormatter.format(format, value);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeVInt(id);
            out.writeNamedWriteable(format);
            out.writeEnum(valueFormatter);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }

            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            final Field that = (Field) other;

            return this.id == that.id
                && this.valueFormatter.ordinal() == that.valueFormatter.ordinal()
                && Objects.equals(this.name, that.name)
                && Objects.equals(this.format, that.format);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, valueFormatter, name, format);
        }

    };

    private final Field field;

    abstract Tuple<Field, List<Object>> collect(LeafReaderContext ctx, int doc) throws IOException;

    ItemSetMapReduceValueSource(ValuesSourceConfig config, int id, ValueFormatter valueFormatter) {
        String fieldName = config.fieldContext() != null ? config.fieldContext().field() : null;

        if (Strings.isNullOrEmpty(fieldName)) {
            throw new IllegalArgumentException("scripts are not supported");
        }

        this.field = new Field(fieldName, id, config.format(), valueFormatter);
    }

    Field getField() {
        return field;
    }

    public static class KeywordValueSource extends ItemSetMapReduceValueSource {
        private final ValuesSource.Bytes source;

        public KeywordValueSource(ValuesSourceConfig config, int id) {
            super(config, id, ValueFormatter.BYTES_REF);
            this.source = (Bytes) config.getValuesSource();
        }

        @Override
        public Tuple<Field, List<Object>> collect(LeafReaderContext ctx, int doc) throws IOException {
            SortedBinaryDocValues values = source.bytesValues(ctx);

            if (values.advanceExact(doc)) {
                int valuesCount = values.docValueCount();
                List<Object> objects = new ArrayList<>(valuesCount);

                for (int i = 0; i < valuesCount; ++i) {
                    objects.add(BytesRef.deepCopyOf(values.nextValue()));
                }
                return new Tuple<>(getField(), objects);
            }
            return new Tuple<>(getField(), Collections.emptyList());
        }
    }

    public static class NumericValueSource extends ItemSetMapReduceValueSource {
        private final ValuesSource.Numeric source;

        public NumericValueSource(ValuesSourceConfig config, int id) {
            super(config, id, ValueFormatter.LONG);
            this.source = (Numeric) config.getValuesSource();
        }

        @Override
        public Tuple<Field, List<Object>> collect(LeafReaderContext ctx, int doc) throws IOException {
            SortedNumericDocValues values = source.longValues(ctx);

            if (values.advanceExact(doc)) {
                int valuesCount = values.docValueCount();
                List<Object> objects = new ArrayList<>(valuesCount);

                for (int i = 0; i < valuesCount; ++i) {
                    objects.add(values.nextValue());
                }
                return new Tuple<>(getField(), objects);
            }
            return new Tuple<>(getField(), Collections.emptyList());
        }

    }
}
