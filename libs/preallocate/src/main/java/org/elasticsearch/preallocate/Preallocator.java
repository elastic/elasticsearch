/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.preallocate;

import java.io.IOException;

/**
 * Represents platform native methods for pre-allocating files.
 */
interface Preallocator {

    /** A handle for an open file */
    interface NativeFileHandle extends AutoCloseable {
        /** A valid native file descriptor */
        int fd();

        /** Retrieves the current size of the file */
        long getSize() throws IOException;
    }

    /**
     * Returns if native methods for pre-allocating files are available.
     *
     * @return true if native methods are available, otherwise false
     */
    boolean useNative();

    /**
     * Open a file for preallocation.
     *
     * @param path The absolute path to the file to be opened
     * @return a handle to the open file that may be used for preallocate
     */
    NativeFileHandle open(String path) throws IOException;

    /**
     * Pre-allocate a file of given current size to the specified size using the given file descriptor.
     *
     * @param fd the file descriptor
     * @param currentSize the current size of the file
     * @param fileSize the size to pre-allocate
     * @return 0 upon success
     */
    int preallocate(int fd, long currentSize, long fileSize);

    /**
     * Provide a string representation of the given error number.
     *
     * @param errno the error number
     * @return the error message
     */
    String error(int errno);

}
