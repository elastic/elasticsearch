/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess.jdk;

import org.elasticsearch.nativeaccess.lib.JavaLibrary;
import org.elasticsearch.nativeaccess.lib.Kernel32Library;
import org.elasticsearch.nativeaccess.lib.LinuxCLibrary;
import org.elasticsearch.nativeaccess.lib.MacCLibrary;
import org.elasticsearch.nativeaccess.lib.NativeLibraryProvider;
import org.elasticsearch.nativeaccess.lib.PosixCLibrary;
import org.elasticsearch.nativeaccess.lib.VectorLibrary;
import org.elasticsearch.nativeaccess.lib.ZstdLibrary;

import java.util.Map;

public class JdkNativeLibraryProvider extends NativeLibraryProvider {

    public JdkNativeLibraryProvider() {
        super(
            "jdk",
            Map.of(
                JavaLibrary.class,
                JdkJavaLibrary::new,
                PosixCLibrary.class,
                JdkPosixCLibrary::new,
                LinuxCLibrary.class,
                JdkLinuxCLibrary::new,
                MacCLibrary.class,
                JdkMacCLibrary::new,
                Kernel32Library.class,
                JdkKernel32Library::new,
                ZstdLibrary.class,
                JdkZstdLibrary::new,
                VectorLibrary.class,
                JdkVectorLibrary::new
            )
        );
    }
}
