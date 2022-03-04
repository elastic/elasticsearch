/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;

public final class FieldSupplier {

    public interface Supplier<T> {

        int size();
        T get(int index);
    }

    public interface BooleanSupplier {

        int size();
        boolean get(int index);
    }

    public interface ByteSupplier {

        int size();
        byte get(int index);
    }

    public interface CharSupplier {

        int size();
        char get(int index);
    }

    public interface IntSupplier {

        int size();
        int get(int index);
    }

    public interface LongSupplier {

        int size();
        long get(int index);
    }

    public interface FloatSupplier {

        int size();
        float get(int index);
    }

    public interface DoubleSupplier {

        int size();
        double get(int index);
    }

    private FieldSupplier() {
        // for namespace only
    }
}
