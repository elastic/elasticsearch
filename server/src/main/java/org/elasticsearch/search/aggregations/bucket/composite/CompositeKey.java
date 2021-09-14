/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Arrays;

/**
 * A key that is composed of multiple {@link Comparable} values.
 */
@SuppressWarnings("rawtypes")
class CompositeKey implements Writeable {
    private final Comparable[] values;

    CompositeKey(Comparable... values) {
        this.values = values;
    }

    CompositeKey(StreamInput in) throws IOException {
        values = in.readArray(i -> (Comparable) i.readGenericValue(), Comparable[]::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray(StreamOutput::writeGenericValue, values);
    }

    Comparable[] values() {
        return values;
    }

    int size() {
        return values.length;
    }

    Comparable get(int pos) {
        assert pos < values.length;
        return values[pos];
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CompositeKey that = (CompositeKey) o;
        return Arrays.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }

    @Override
    public String toString() {
        return "CompositeKey{" + "values=" + Arrays.toString(values) + '}';
    }
}
