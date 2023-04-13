/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.elasticsearch.common.util.iterable.Iterables;

import java.util.stream.Stream;

public interface Streamable<T> extends Iterable<T> {

    default Stream<T> stream() {
        return Iterables.stream(this);
    }

    int size();

    default boolean isEmpty() {
        return size() == 0;
    }
}
