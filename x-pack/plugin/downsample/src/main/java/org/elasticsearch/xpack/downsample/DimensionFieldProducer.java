/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class DimensionFieldProducer extends AbstractDownsampleFieldProducer {
    private final Dimension dimension;

    DimensionFieldProducer(final String name, final Dimension dimension) {
        super(name);
        this.dimension = dimension;
    }

    static class Dimension {
        private final String name;
        private Object value;
        private boolean isEmpty;

        Dimension(String name) {
            this.name = name;
            this.isEmpty = true;
        }

        public Object value() {
            return value;
        }

        public String name() {
            return name;
        }

        void reset() {
            value = null;
            isEmpty = true;
        }

        void collectOnce(final Object value) {
            assert isEmpty;
            Objects.requireNonNull(value);
            this.value = value;
            this.isEmpty = false;
        }

        /**
         * This is an expensive check, that slows down downsampling significantly.
         * Given that index is sorted by tsid as primary key, this shouldn't really happen.
         */
        boolean validate(FormattedDocValues docValues, int docId) throws IOException {
            if (docValues.advanceExact(docId)) {
                int docValueCount = docValues.docValueCount();
                for (int i = 0; i < docValueCount; i++) {
                    var value = docValues.nextValue();
                    if (value.equals(this.value) == false) {
                        assert false : "Dimension value changed without tsid change [" + value + "] != [" + this.value + "]";
                    }
                }
            }

            return true;
        }
    }

    @Override
    public void reset() {
        this.dimension.reset();
    }

    @Override
    public boolean isEmpty() {
        return this.dimension.isEmpty;
    }

    @Override
    public void collect(FormattedDocValues docValues, int docId) throws IOException {
        if (dimension.isEmpty == false) {
            assert dimension.validate(docValues, docId);
            return;
        }

        if (docValues.advanceExact(docId) == false) {
            return;
        }
        int docValueCount = docValues.docValueCount();
        for (int i = 0; i < docValueCount; i++) {
            this.dimension.collectOnce(docValues.nextValue());
        }
    }

    @Override
    public void write(XContentBuilder builder) throws IOException {
        if (isEmpty() == false) {
            builder.field(this.dimension.name, this.dimension.value());
        }
    }
}
