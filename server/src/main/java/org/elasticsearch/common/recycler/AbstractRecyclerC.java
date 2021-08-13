/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.recycler;

public abstract class AbstractRecyclerC<T> implements Recycler.C<T> {

    @Override
    public abstract T newInstance();

    @Override
    public abstract void recycle(T value);

    @Override
    public void destroy(T value) {
        // by default we simply drop the object for GC.
    }

}
