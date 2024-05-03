/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;

import java.io.IOException;

/**
 * Simple diffable object with simple diffs implementation that sends the entire object if object has changed or
 * nothing if object remained the same.
 */
public interface SimpleDiffable<T extends Diffable<T>> extends Diffable<T> {

    Diff<?> EMPTY = new CompleteDiff<>();

    @SuppressWarnings("unchecked")
    @Override
    default Diff<T> diff(T previousState) {
        if (this.equals(previousState)) {
            return empty();
        } else {
            return new CompleteDiff<>((T) this);
        }
    }

    static <T extends Diffable<T>> Diff<T> readDiffFrom(Reader<T> reader, StreamInput in) throws IOException {
        if (in.readBoolean()) {
            return new CompleteDiff<>(reader.read(in));
        }
        return empty();
    }

    /**
     * @return empty diff instance that returns the input object when applied
     */
    @SuppressWarnings("unchecked")
    static <V> Diff<V> empty() {
        return (Diff<V>) EMPTY;
    }

    class CompleteDiff<T extends Diffable<T>> implements Diff<T> {

        @Nullable
        private final T part;

        /**
         * Creates simple diff with changes
         */
        CompleteDiff(T part) {
            this.part = part;
        }

        /**
         * Creates simple diff without changes
         */
        CompleteDiff() {
            this.part = null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (part != null) {
                out.writeBoolean(true);
                part.writeTo(out);
            } else {
                out.writeBoolean(false);
            }
        }

        @Override
        public T apply(T part) {
            if (this.part != null) {
                return this.part;
            } else {
                return part;
            }
        }
    }
}
