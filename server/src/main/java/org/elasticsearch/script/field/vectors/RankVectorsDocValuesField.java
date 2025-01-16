/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script.field.vectors;

import org.elasticsearch.index.mapper.vectors.RankVectorsScriptDocValues;
import org.elasticsearch.script.field.AbstractScriptFieldFactory;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.script.field.Field;

import java.util.Iterator;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;

public abstract class RankVectorsDocValuesField extends AbstractScriptFieldFactory<RankVectors>
    implements
        Field<RankVectors>,
        DocValuesScriptFieldFactory,
        RankVectorsScriptDocValues.RankVectorsSupplier {
    protected final String name;
    protected final ElementType elementType;

    public RankVectorsDocValuesField(String name, ElementType elementType) {
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
    public abstract RankVectors get();

    public abstract RankVectors get(RankVectors defaultValue);

    public abstract RankVectorsScriptDocValues toScriptDocValues();

    // DenseVector fields are single valued, so Iterable does not make sense.
    @Override
    public Iterator<RankVectors> iterator() {
        throw new UnsupportedOperationException("Cannot iterate over single valued rank_vectors field, use get() instead");
    }
}
