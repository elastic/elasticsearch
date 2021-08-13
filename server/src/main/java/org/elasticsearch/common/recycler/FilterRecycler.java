/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.recycler;

abstract class FilterRecycler<T> implements Recycler<T> {

    /** Get the delegate instance to forward calls to. */
    protected abstract Recycler<T> getDelegate();

    /** Wrap a recycled reference. */
    protected Recycler.V<T> wrap(Recycler.V<T> delegate) {
        return delegate;
    }

    @Override
    public Recycler.V<T> obtain() {
        return wrap(getDelegate().obtain());
    }

}
