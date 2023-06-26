/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.elasticsearch.common.geo.BoundingBox;
import org.elasticsearch.common.geo.SpatialPoint;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.script.field.ToScriptFieldFactory;

/**
 * {@link LeafFieldData} specialization for geo-shapes and shapes.
 */
public abstract class LeafShapeFieldData<T extends ShapeValues<?>> implements LeafFieldData {
    protected final ToScriptFieldFactory<T> toScriptFieldFactory;

    public LeafShapeFieldData(ToScriptFieldFactory<T> toScriptFieldFactory) {
        this.toScriptFieldFactory = toScriptFieldFactory;
    }

    public static class Empty<T extends ShapeValues<?>> extends LeafShapeFieldData<T> {
        private final T emptyValues;

        public Empty(ToScriptFieldFactory<T> toScriptFieldFactory, T emptyValues) {
            super(toScriptFieldFactory);
            this.emptyValues = emptyValues;
        }

        @Override
        public long ramBytesUsed() {
            return 0;
        }

        @Override
        public void close() {}

        @Override
        public T getShapeValues() {
            return emptyValues;
        }
    }

    /**
     * Return geo-shape or shape values.
     */
    public abstract T getShapeValues();

    @Override
    public final SortedBinaryDocValues getBytesValues() {
        throw new UnsupportedOperationException("scripts and term aggs are not supported by geo_shape or shape doc values");
    }

    @Override
    public final DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
        return toScriptFieldFactory.getScriptFieldFactory(getShapeValues(), name);
    }

    public static class ShapeScriptValues<T extends SpatialPoint, V extends ShapeValues.ShapeValue> extends ScriptDocValues.BaseGeometry<
        T,
        V> {

        private final GeometrySupplier<T, V> gsSupplier;

        protected ShapeScriptValues(GeometrySupplier<T, V> supplier) {
            super(supplier);
            this.gsSupplier = supplier;
        }

        @Override
        public int getDimensionalType() {
            return gsSupplier.getInternal(0) == null ? -1 : gsSupplier.getInternal(0).dimensionalShapeType().ordinal();
        }

        @Override
        public T getCentroid() {
            return gsSupplier.getInternal(0) == null ? null : gsSupplier.getInternalCentroid();
        }

        @Override
        public BoundingBox<T> getBoundingBox() {
            return gsSupplier.getInternal(0) == null ? null : gsSupplier.getInternalBoundingBox();
        }

        @Override
        public T getLabelPosition() {
            return gsSupplier.getInternal(0) == null ? null : gsSupplier.getInternalLabelPosition();
        }

        @Override
        public V get(int index) {
            return gsSupplier.getInternal(0);
        }

        public V getValue() {
            return gsSupplier.getInternal(0);
        }

        @Override
        public int size() {
            return supplier.size();
        }
    }
}
