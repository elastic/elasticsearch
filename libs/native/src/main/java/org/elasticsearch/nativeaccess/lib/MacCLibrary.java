/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.lib;

public non-sealed interface MacCLibrary extends NativeLibrary {

    interface ErrorReference {}

    ErrorReference newErrorReference();

    /**
     * maps to sandbox_init(3), since Leopard
     */
    int sandbox_init(String profile, long flags, ErrorReference errorbuf);

    /**
     * releases memory when an error occurs during initialization (e.g. syntax bug)
     */
    void sandbox_free_error(ErrorReference errorbuf);

    interface FStore {
        void set_flags(int flags); /* IN: flags word */

        void set_posmode(int posmode); /* IN: indicates offset field */

        void set_offset(long offset); /* IN: start of the region */

        void set_length(long length); /* IN: size of the region */

        long bytesalloc(); /* OUT: number of bytes allocated */
    }

    FStore newFStore();

    int fcntl(int fd, int cmd, FStore fst);

    int ftruncate(int fd, long length);
}
