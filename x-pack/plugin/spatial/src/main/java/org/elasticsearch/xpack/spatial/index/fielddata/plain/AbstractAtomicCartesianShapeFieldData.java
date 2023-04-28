/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata.plain;

import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;
import org.elasticsearch.xpack.spatial.index.fielddata.CartesianShapeValues;
import org.elasticsearch.xpack.spatial.index.fielddata.LeafShapeFieldData;

public abstract class AbstractAtomicCartesianShapeFieldData extends LeafShapeFieldData<CartesianShapeValues> {

    public AbstractAtomicCartesianShapeFieldData(ToScriptFieldFactory<CartesianShapeValues> toScriptFieldFactory) {
        super(toScriptFieldFactory);
    }

    public static LeafShapeFieldData<CartesianShapeValues> empty(
        final int maxDoc,
        ToScriptFieldFactory<CartesianShapeValues> toScriptFieldFactory
    ) {
        return new Empty<>(toScriptFieldFactory, CartesianShapeValues.EMPTY);
    }

    public static final class CartesianShapeScriptValues extends LeafShapeFieldData.ShapeScriptValues<
        CartesianPoint,
        CartesianShapeValues.CartesianShapeValue> {

        public CartesianShapeScriptValues(GeometrySupplier<CartesianPoint, CartesianShapeValues.CartesianShapeValue> supplier) {
            super(supplier);
        }
    }
}
