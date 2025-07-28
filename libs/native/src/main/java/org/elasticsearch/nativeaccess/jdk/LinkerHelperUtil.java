/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess.jdk;

import java.lang.foreign.Linker;

public class LinkerHelperUtil {

    static final Linker.Option[] NONE = new Linker.Option[0];

    /** Returns an empty linker option array, since critical is only available since Java 22. */
    static Linker.Option[] critical() {
        return NONE;
    }

    private LinkerHelperUtil() {}
}
