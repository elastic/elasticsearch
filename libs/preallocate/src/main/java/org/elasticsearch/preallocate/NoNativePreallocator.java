/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.preallocate;

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
