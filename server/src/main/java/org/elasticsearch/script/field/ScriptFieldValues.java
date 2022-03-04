/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

public class ScriptFieldValues {

    public interface Values<T> {

        int size();
        T get(int index);
    }

    public interface BooleanValues {

        int size();
        boolean get(int index);
    }

    public interface ByteValues {

        int size();
        byte get(int index);
    }

    public interface CharValues {

        int size();
        char get(int index);
    }

    public interface IntValues {

        int size();
        int get(int index);
    }

    public interface LongValues {

        int size();
        long get(int index);
    }

    public interface FloatValues {

        int size();
        float get(int index);
    }

    public interface DoubleValues {

        int size();
        double get(int index);
    }
}
