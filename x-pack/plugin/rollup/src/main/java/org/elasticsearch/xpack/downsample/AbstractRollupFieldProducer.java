/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.io.IOException;

/**
 * Base class that reads fields from the source index and produces their downsampled values
 */
abstract class AbstractRollupFieldProducer implements RollupFieldSerializer {

    private final String name;
    protected MappedFieldType fieldType;
    protected boolean isEmpty;

    AbstractRollupFieldProducer(final MappedFieldType fieldType, final String name) {
        this.name = name;
        this.fieldType = fieldType;
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

    public abstract void collect(LeafFieldData leafFieldData, int docId) throws IOException;
}
