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

        void collect(final Object value) {
            Objects.requireNonNull(value);
            if (isEmpty) {
                this.value = value;
                this.isEmpty = false;
                return;
            }
            if (value.equals(this.value) == false) {
                throw new IllegalArgumentException("Dimension value changed without tsid change [" + value + "] != [" + this.value + "]");
            }
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
        if (docValues.advanceExact(docId) == false) {
            return;
        }
        int docValueCount = docValues.docValueCount();
        for (int i = 0; i < docValueCount; i++) {
            this.dimension.collect(docValues.nextValue());
        }
    }

    @Override
    public void write(XContentBuilder builder) throws IOException {
        if (isEmpty() == false) {
            builder.field(this.dimension.name, this.dimension.value());
        }
    }
}
