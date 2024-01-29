/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jna;

import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;
import com.sun.jna.ptr.PointerByReference;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.nativeaccess.lib.MacCLibrary.ErrorReference;
import org.elasticsearch.nativeaccess.lib.MacCLibrary.FStore;

class JnaStaticMacCLibrary {

    private static final Logger logger = LogManager.getLogger(JnaStaticMacCLibrary.class);

    public static final boolean loaded;

    static {
        boolean success = false;
        try {
            Native.register("c");
            success = true;
        } catch (UnsatisfiedLinkError e) {
            logger.warn("unable to link C library. native methods will be disabled.", e);
        }
        loaded = success;
    }

    static class JnaErrorReference implements ErrorReference {
        final PointerByReference ref = new PointerByReference();

        @Override
        public String toString() {
            return ref.getValue().getString(0);
        }
    }

    /**
     * maps to sandbox_init(3), since Leopard
     */
    static native int sandbox_init(String profile, long flags, PointerByReference errorbuf);

    /**
     * releases memory when an error occurs during initialization (e.g. syntax bug)
     */
    static native void sandbox_free_error(Pointer errorbuf);

    public static class JnaFStore extends Structure implements Structure.ByReference, FStore {

        public int fst_flags = 0;
        public int fst_posmode = 0;
        public NativeLong fst_offset = new NativeLong(0);
        public NativeLong fst_length = new NativeLong(0);
        public NativeLong fst_bytesalloc = new NativeLong(0);

        @Override
        public void set_flags(int flags) {
            this.fst_flags = flags;
        }

        @Override
        public void set_posmode(int posmode) {
            this.fst_posmode = posmode;
        }

        @Override
        public void set_offset(long offset) {
            fst_offset.setValue(offset);
        }

        @Override
        public void set_length(long length) {
            fst_length.setValue(length);
        }

        @Override
        public long bytesalloc() {
            return fst_bytesalloc.longValue();
        }
    }

    static native int fcntl(int fd, int cmd, JnaFStore fst);

    static native int ftruncate(int fd, NativeLong length);
}
