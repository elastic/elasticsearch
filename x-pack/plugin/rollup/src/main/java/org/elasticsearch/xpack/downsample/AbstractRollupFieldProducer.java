/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Base class for classes that read metric and label fields.
 */
abstract class AbstractRollupFieldProducer<T> {

    protected final String name;
    protected boolean isEmpty;

    AbstractRollupFieldProducer(String name) {
        this.name = name;
        this.isEmpty = true;
    }

    /**
     * @return the name of the field.
     */
    public String name() {
        return name;
    }

    /**
     * Resets the producer to an empty value.
     */
    public abstract void reset();

    /**
     * Serialize the downsampled value of the field.
     */
    public abstract void write(XContentBuilder builder) throws IOException;

    /**
     * @return true if the field has not collected any value.
     */
    public boolean isEmpty() {
        return isEmpty;
    }

    @FunctionalInterface interface LeafCollector {
        void collect(int doc) throws IOException;
    }

    public abstract LeafCollector leaf(LeafReaderContext ctx) throws IOException;
}
