/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.recycler;

import org.elasticsearch.core.Releasable;

/**
 * A recycled object, note, implementations should support calling obtain and then recycle
 * on different threads.
 */
public interface Recycler<T> {

    interface Factory<T> {
        Recycler<T> build();
    }

    interface C<T> {

        /** Create a new empty instance of the given size. */
        T newInstance();

        /** Recycle the data. This operation is called when the data structure is released. */
        void recycle(T value);

        /** Destroy the data. This operation allows the data structure to release any internal resources before GC. */
        void destroy(T value);
    }

    interface V<T> extends Releasable {

        /** Reference to the value. */
        T v();

        /** Whether this instance has been recycled (true) or newly allocated (false). */
        boolean isRecycled();

    }

    V<T> obtain();

}
