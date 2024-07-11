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
}
