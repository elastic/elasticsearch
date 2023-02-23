/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.index.fielddata.FormattedDocValues;

import java.io.IOException;

/**
 * Base class that reads fields from the source index and produces their downsampled values
 */
abstract class AbstractDownsampleFieldProducer implements DownsampleFieldSerializer {

    private final String name;
    protected boolean isEmpty;

    AbstractDownsampleFieldProducer(String name) {
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
     * @return true if the field has not collected any value.
     */
    public boolean isEmpty() {
        return isEmpty;
    }

    public abstract void collect(FormattedDocValues docValues, int docId) throws IOException;
}
