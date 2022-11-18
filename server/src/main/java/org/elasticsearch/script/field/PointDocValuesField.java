/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.common.geo.BoundingBox;
import org.elasticsearch.common.geo.SpatialPoint;
import org.elasticsearch.index.fielddata.MultiPointValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

public abstract class PointDocValuesField<T extends SpatialPoint> extends AbstractScriptFieldFactory<T>
    implements
        Field<T>,
        DocValuesScriptFieldFactory,
        ScriptDocValues.GeometrySupplier<T, T> {

    protected final MultiPointValues<T> input;
    protected final String name;

    protected T[] values;
    protected int count;

    private final Supplier<T> pointMaker;
    protected final T centroid;
    protected final BoundingBox<T> boundingBox;
    private int labelIndex = 0;

    public PointDocValuesField(MultiPointValues<T> input, String name, Supplier<T> pointMaker, BoundingBox<T> boundingBox, T[] values) {
        this.input = input;
        this.name = name;
        this.pointMaker = pointMaker;
        this.centroid = pointMaker.get();
        this.boundingBox = boundingBox;
        this.values = values;
    }

    @Override
    public void setNextDocId(int docId) throws IOException {
        if (input.advanceExact(docId)) {
            resize(input.docValueCount());
            if (count == 1) {
                setSingleValue();
            } else {
                setMultiValue();
            }
        } else {
            resize(0);
        }
    }

    private void resize(int newSize) {
        count = newSize;
        if (newSize > values.length) {
            int oldLength = values.length;
            values = ArrayUtil.grow(values, count);
            for (int i = oldLength; i < values.length; ++i) {
                values[i] = pointMaker.get();
            }
        }
    }

    protected abstract void resetPointAt(int i, T point);

    protected abstract void resetCentroidAndBounds(T centroid, T topLeft, T bottomRight);

    protected abstract double getXFrom(T point);

    protected abstract double getYFrom(T point);

    protected abstract T pointOf(double x, double y);

    protected abstract double planeDistance(double x1, double y1, T point);

    private void setSingleValue() throws IOException {
        T point = input.nextValue();
        resetPointAt(0, point);
        resetCentroidAndBounds(point, point, point);
        labelIndex = 0;
    }

    private void setMultiValue() throws IOException {
        double centroidY = 0;
        double centroidX = 0;
        labelIndex = 0;
        double maxX = Double.NEGATIVE_INFINITY;
        double minX = Double.POSITIVE_INFINITY;
        double maxY = Double.NEGATIVE_INFINITY;
        double minY = Double.POSITIVE_INFINITY;
        for (int i = 0; i < count; i++) {
            T point = input.nextValue();
            resetPointAt(i, point);
            centroidX += getXFrom(point);
            centroidY += getYFrom(point);
            maxX = Math.max(maxX, getXFrom(values[i]));
            minX = Math.min(minX, getXFrom(values[i]));
            maxY = Math.max(maxY, getYFrom(values[i]));
            minY = Math.min(minY, getYFrom(values[i]));
            labelIndex = closestPoint(labelIndex, i, (minX + maxX) / 2, (minY + maxY) / 2);
        }
        resetCentroidAndBounds(pointOf(centroidX, centroidY), pointOf(minX, maxY), pointOf(maxX, minY));
    }

    private int closestPoint(int a, int b, double x, double y) {
        if (a == b) {
            return a;
        }
        double distA = planeDistance(x, y, values[a]);
        double distB = planeDistance(x, y, values[b]);
        return distA < distB ? a : b;
    }

    @Override
    public T getInternal(int index) {
        return values[index];
    }

    // maintain bwc by making centroid available to ScriptDocValues.GeoPoints
    @Override
    public T getInternalCentroid() {
        return centroid;
    }

    // maintain bwc by making bounding box available to ScriptDocValues.GeoPoints
    @Override
    public BoundingBox<T> getInternalBoundingBox() {
        return boundingBox;
    }

    @Override
    public T getInternalLabelPosition() {
        return values[labelIndex];
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isEmpty() {
        return count == 0;
    }

    @Override
    public int size() {
        return count;
    }

    public T get(T defaultValue) {
        return get(0, defaultValue);
    }

    public T get(int index, T defaultValue) {
        if (isEmpty() || index < 0 || index >= count) {
            return defaultValue;
        }

        return values[index];
    }

    @Override
    public Iterator<T> iterator() {
        return new Iterator<>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < count;
            }

            @Override
            public T next() {
                if (hasNext() == false) {
                    throw new NoSuchElementException();
                }
                return values[index++];
            }
        };
    }
}
