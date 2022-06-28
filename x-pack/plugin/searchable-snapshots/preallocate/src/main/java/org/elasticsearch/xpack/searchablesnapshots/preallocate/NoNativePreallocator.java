/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.preallocate;

final class NoNativePreallocator implements Preallocator {

    @Override
    public boolean useNative() {
        return false;
    }

    @Override
    public int preallocate(final int fd, final long currentSize, final long fileSize) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String error(final int errno) {
        throw new UnsupportedOperationException();
    }

}
