/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.util;

import java.util.function.BiConsumer;

public interface ThrowableBiConsumer<T, U> extends BiConsumer<T, U> {
    // NOCOMMIT replace with CheckedBiConsumer

    @Override
    default void accept(T t, U u) {
        try {
            acceptThrows(t, u);
        } catch (Exception ex) {
            throw new WrappingException(ex);
        }
    }

    void acceptThrows(T t, U u) throws Exception;
}