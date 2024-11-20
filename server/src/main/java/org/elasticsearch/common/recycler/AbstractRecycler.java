/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.recycler;

abstract class AbstractRecycler<T> implements Recycler<T> {

    protected final Recycler.C<T> c;

    protected AbstractRecycler(Recycler.C<T> c) {
        this.c = c;
    }

    @Override
    public int pageSize() {
        return c.pageSize();
    }
}
