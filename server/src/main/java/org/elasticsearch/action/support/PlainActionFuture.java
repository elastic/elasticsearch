/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.core.CheckedConsumer;

public class PlainActionFuture<T> extends AdapterActionFuture<T, T> {

    public static <T> PlainActionFuture<T> newFuture() {
        return new PlainActionFuture<>();
    }

    public static <T, E extends Exception> T get(CheckedConsumer<PlainActionFuture<T>, E> e) throws E {
        PlainActionFuture<T> fut = newFuture();
        e.accept(fut);
        return fut.actionGet();
    }

    @Override
    protected T convert(T listenerResponse) {
        return listenerResponse;
    }
}
