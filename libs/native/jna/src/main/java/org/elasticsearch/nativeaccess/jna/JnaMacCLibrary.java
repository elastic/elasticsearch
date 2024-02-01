/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jna;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;
import com.sun.jna.ptr.PointerByReference;

import org.elasticsearch.nativeaccess.lib.MacCLibrary;

class JnaMacCLibrary implements MacCLibrary {

    static class JnaErrorReference implements ErrorReference {
        final PointerByReference ref = new PointerByReference();

        @Override
        public String toString() {
            return ref.getValue().getString(0);
        }
    }

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

    private interface NativeFunctions extends Library {
        int sandbox_init(String profile, long flags, PointerByReference errorbuf);

        void sandbox_free_error(Pointer errorbuf);

        int fcntl(int fd, int cmd, JnaFStore fst);

        int ftruncate(int fd, NativeLong length);
    }

    private final NativeFunctions functions;

    JnaMacCLibrary() {
        this.functions = Native.load("c", NativeFunctions.class);
    }

    @Override
    public ErrorReference newErrorReference() {
        return new JnaErrorReference();
    }

    @Override
    public int sandbox_init(String profile, long flags, ErrorReference errorbuf) {
        assert errorbuf instanceof JnaErrorReference;
        var jnaErrorbuf = (JnaErrorReference) errorbuf;
        return functions.sandbox_init(profile, flags, jnaErrorbuf.ref);
    }

    @Override
    public void sandbox_free_error(ErrorReference errorbuf) {
        assert errorbuf instanceof JnaErrorReference;
        var jnaErrorbuf = (JnaErrorReference) errorbuf;
        functions.sandbox_free_error(jnaErrorbuf.ref.getValue());
    }

    @Override
    public FStore newFStore() {
        return new JnaFStore();
    }

    @Override
    public int fcntl(int fd, int cmd, FStore fst) {
        assert fst instanceof JnaFStore;
        var jnaFst = (JnaFStore) fst;
        return functions.fcntl(fd, cmd, jnaFst);
    }

    @Override
    public int ftruncate(int fd, long length) {
        return functions.ftruncate(fd, new NativeLong(length));
    }
}
