/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.lib;

/**
 * Provides access to methods in libc.so available on POSIX systems.
 */
public non-sealed interface PosixCLibrary extends NativeLibrary {

    /**
     * Gets the effective userid of the current process.
     *
     * @return the effective user id
     * @see <a href="https://pubs.opengroup.org/onlinepubs/9699919799/functions/geteuid.html">geteuid</a>
     */
    int geteuid();
}
