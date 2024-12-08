/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script.field.vectors;

import org.elasticsearch.index.mapper.vectors.MultiDenseVectorScriptDocValues;
import org.elasticsearch.script.field.AbstractScriptFieldFactory;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.script.field.Field;

import java.util.Iterator;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;

public abstract class MultiDenseVectorDocValuesField extends AbstractScriptFieldFactory<MultiDenseVector>
    implements
        Field<MultiDenseVector>,
        DocValuesScriptFieldFactory,
        MultiDenseVectorScriptDocValues.MultiDenseVectorSupplier {
    protected final String name;
    protected final ElementType elementType;

    public MultiDenseVectorDocValuesField(String name, ElementType elementType) {
        this.name = name;
        this.elementType = elementType;
    }

    @Override
    public String getName() {
        return name;
    }

    public ElementType getElementType() {
        return elementType;
    }

    /**
     * Get the DenseVector for a document if one exists, DenseVector.EMPTY otherwise
     */
    public abstract MultiDenseVector get();

    public abstract MultiDenseVector get(MultiDenseVector defaultValue);

    public abstract MultiDenseVectorScriptDocValues toScriptDocValues();

    // DenseVector fields are single valued, so Iterable does not make sense.
    @Override
    public Iterator<MultiDenseVector> iterator() {
        throw new UnsupportedOperationException("Cannot iterate over single valued rank_vectors field, use get() instead");
    }
}
