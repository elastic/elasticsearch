/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

public class ValuesSuppliers {

    public interface ValueSupplier<T> {

        int size();
        T get(int index);
    }

    public interface BooleanValueSupplier {

        int size();
        boolean get(int index);
    }

    public interface ByteValueSupplier {

        int size();
        byte get(int index);
    }

    public interface CharValueSupplier {

        int size();
        char get(int index);
    }

    public interface IntValueSupplier {

        int size();
        int get(int index);
    }

    public interface LongValueSupplier {

        int size();
        long get(int index);
    }

    public interface FloatValueSupplier {

        int size();
        float get(int index);
    }

    public interface DoubleValueSupplier {

        int size();
        double get(int index);
    }
}
