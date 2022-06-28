/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.geo.BoundingBox;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.xcontent.ToXContentFragment;

import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.common.geo.SphericalMercatorUtils.latToSphericalMercator;
import static org.elasticsearch.common.geo.SphericalMercatorUtils.lonToSphericalMercator;

/**
 * {@link LeafFieldData} specialization for geo-shapes and shapes.
 */
public abstract class LeafShapeFieldData<T extends ToXContentFragment> implements LeafFieldData {
    protected final ToScriptFieldFactory<ShapeValues<T>> toScriptFieldFactory;

    public LeafShapeFieldData(ToScriptFieldFactory<ShapeValues<T>> toScriptFieldFactory) {
        this.toScriptFieldFactory = toScriptFieldFactory;
    }

    public static class Empty<T extends ToXContentFragment> extends LeafShapeFieldData<T> {
        private final ShapeValues<T> emptyValues;

        public Empty(ToScriptFieldFactory<ShapeValues<T>> toScriptFieldFactory, ShapeValues<T> emptyValues) {
            super(toScriptFieldFactory);
            this.emptyValues = emptyValues;
        }

        @Override
        public long ramBytesUsed() {
            return 0;
        }

        @Override
        public Collection<Accountable> getChildResources() {
            return Collections.emptyList();
        }

        @Override
        public void close() {}

        @Override
        public ShapeValues<T> getShapeValues() {
            return emptyValues;
        }
    }

    /**
     * Return geo-shape or shape values.
     */
    public abstract ShapeValues<T> getShapeValues();

    @Override
    public final SortedBinaryDocValues getBytesValues() {
        throw new UnsupportedOperationException("scripts and term aggs are not supported by geo_shape or shape doc values");
    }

    @Override
    public final DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
        return toScriptFieldFactory.getScriptFieldFactory(getShapeValues(), name);
    }

    public static class ShapeScriptValues<T extends ToXContentFragment> extends ScriptDocValues.Geometry<T, ShapeValues.ShapeValue<T>> {

        private final GeometrySupplier<T, ShapeValues.ShapeValue<T>> gsSupplier;

        protected ShapeScriptValues(GeometrySupplier<T, ShapeValues.ShapeValue<T>> supplier) {
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
        public double getMercatorWidth() {
            return lonToSphericalMercator(getBoundingBox().right()) - lonToSphericalMercator(getBoundingBox().left());
        }

        @Override
        public double getMercatorHeight() {
            return latToSphericalMercator(getBoundingBox().top()) - latToSphericalMercator(getBoundingBox().bottom());
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
        public ShapeValues.ShapeValue<T> get(int index) {
            return gsSupplier.getInternal(0);
        }

        public ShapeValues.ShapeValue<T> getValue() {
            return gsSupplier.getInternal(0);
        }

        @Override
        public int size() {
            return supplier.size();
        }
    }
}
