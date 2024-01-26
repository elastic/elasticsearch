/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jna;

import com.sun.jna.Native;
import com.sun.jna.Structure;

import org.elasticsearch.nativeaccess.lib.LinuxCLibrary.statx;

class JnaStaticLinuxCLibrary {

    static {
        Native.register("c");
    }

    /** corresponds to struct statx */
    @Structure.FieldOrder({"_ignore1", "stx_blocks", "_ignore2"})
    public static final class JnaStatx extends Structure implements Structure.ByReference, statx {
        // ignoring bytes up to stx_blocks, including 2 bytes padding after stx_mode for alignment
        public byte[] _ignore1 = new byte[48];
        public long stx_blocks = 0;
        // ignoring bytes after stx_blocks. statx_timestamp is 16 bytes each for alignment
        public byte[] _ignore2 = new byte[104];

        @Override
        public long stx_blocks() {
            return stx_blocks;
        }
    }

    static native int statx(int dirfd, String pathname, int flags, int mask, JnaStatx statxbuf);
}
