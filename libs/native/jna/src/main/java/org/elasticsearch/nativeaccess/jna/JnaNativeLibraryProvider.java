/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jna;

import org.elasticsearch.nativeaccess.lib.JavaLibrary;
import org.elasticsearch.nativeaccess.lib.Kernel32Library;
import org.elasticsearch.nativeaccess.lib.LinuxCLibrary;
import org.elasticsearch.nativeaccess.lib.MacCLibrary;
import org.elasticsearch.nativeaccess.lib.NativeLibrary;
import org.elasticsearch.nativeaccess.lib.NativeLibraryProvider;
import org.elasticsearch.nativeaccess.lib.PosixCLibrary;
import org.elasticsearch.nativeaccess.lib.SystemdLibrary;
import org.elasticsearch.nativeaccess.lib.VectorLibrary;
import org.elasticsearch.nativeaccess.lib.ZstdLibrary;

import java.util.Map;
import java.util.function.Supplier;

public class JnaNativeLibraryProvider extends NativeLibraryProvider {

    public JnaNativeLibraryProvider() {
        super(
            "jna",
            Map.of(
                JavaLibrary.class,
                JnaJavaLibrary::new,
                PosixCLibrary.class,
                JnaPosixCLibrary::new,
                LinuxCLibrary.class,
                JnaLinuxCLibrary::new,
                MacCLibrary.class,
                JnaMacCLibrary::new,
                Kernel32Library.class,
                JnaKernel32Library::new,
                SystemdLibrary.class,
                JnaSystemdLibrary::new,
                ZstdLibrary.class,
                JnaZstdLibrary::new,
                VectorLibrary.class,
                notImplemented()
            )
        );
    }

    private static Supplier<NativeLibrary> notImplemented() {
        return () -> { throw new AssertionError(); };
    }
}
