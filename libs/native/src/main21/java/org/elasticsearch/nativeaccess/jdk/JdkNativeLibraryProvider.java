/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jdk;

import org.elasticsearch.nativeaccess.lib.NativeLibraryProvider;
import org.elasticsearch.nativeaccess.lib.PosixCLibrary;
import org.elasticsearch.nativeaccess.lib.SystemdLibrary;

import java.util.Map;

public class JdkNativeLibraryProvider extends NativeLibraryProvider {

    public JdkNativeLibraryProvider() {
        super("jdk", Map.of(PosixCLibrary.class, JdkPosixCLibrary::new, SystemdLibrary.class, JdkSystemdLibrary::new));
    }
}
