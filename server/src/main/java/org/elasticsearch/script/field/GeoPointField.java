/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.elasticsearch.common.geo.GeoPoint;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class GeoPointField implements Field<GeoPoint> {

    protected final String name;
    protected final FieldSupplier.Supplier<GeoPoint> supplier;

    public GeoPointField(String name, FieldSupplier.Supplier<GeoPoint> supplier) {
        this.name = name;
        this.supplier = supplier;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isEmpty() {
        return supplier.size() == 0;
    }

    @Override
    public int size() {
        return supplier.size();
    }

    public GeoPoint get(GeoPoint defaultValue) {
        return get(0, defaultValue);
    }

    public GeoPoint get(int index, GeoPoint defaultValue) {
        if (isEmpty() || index < 0 || index >= supplier.size()) {
            return defaultValue;
        }

        return supplier.get(index);
    }

    @Override
    public Iterator<GeoPoint> iterator() {
        return new Iterator<GeoPoint>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < supplier.size();
            }

            @Override
            public GeoPoint next() {
                if (hasNext() == false) {
                    throw new NoSuchElementException();
                }
                return supplier.get(index++);
            }
        };
    }
}
