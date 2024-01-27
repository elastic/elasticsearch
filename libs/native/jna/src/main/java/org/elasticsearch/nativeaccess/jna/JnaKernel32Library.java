/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jna;

import com.sun.jna.Pointer;

import com.sun.jna.WString;

import com.sun.jna.ptr.IntByReference;

import org.elasticsearch.nativeaccess.jna.JnaStaticKernel32Library.JnaMemoryBasicInformation;
import org.elasticsearch.nativeaccess.jna.JnaStaticKernel32Library.SizeT;
import org.elasticsearch.nativeaccess.lib.Kernel32Library;

import java.util.function.IntConsumer;

public class JnaKernel32Library implements Kernel32Library {

    @Override
    public long GetCurrentProcess() {
        return Pointer.nativeValue(JnaStaticKernel32Library.GetCurrentProcess());
    }

    @Override
    public boolean CloseHandle(long handle) {
        return JnaStaticKernel32Library.CloseHandle(new Pointer(handle));
    }

    @Override
    public int GetLastError() {
        return JnaStaticKernel32Library.GetLastError();
    }

    @Override
    public MemoryBasicInformation newMemoryBasicInformation() {
        return new JnaMemoryBasicInformation();
    }

    @Override
    public boolean VirtualLock(long address, long size) {
        return JnaStaticKernel32Library.VirtualLock(new Pointer(address), new SizeT(size));
    }

    @Override
    public int VirtualQueryEx(long processHandle, long address, MemoryBasicInformation memoryInfo) {
        assert memoryInfo instanceof JnaMemoryBasicInformation;
        var jnaMemoryInfo = (JnaMemoryBasicInformation) memoryInfo;
        return JnaStaticKernel32Library.VirtualQueryEx(
            new Pointer(processHandle),
            new Pointer(address),
            jnaMemoryInfo,
            jnaMemoryInfo.size());
    }

    @Override
    public boolean SetProcessWorkingSetSize(long processHandle, long minSize, long maxSize) {
        return JnaStaticKernel32Library.SetProcessWorkingSetSize(new Pointer(processHandle), new SizeT(minSize), new SizeT(maxSize));
    }

    @Override
    public int GetCompressedFileSizeW(String lpFileName, IntConsumer lpFileSizeHigh) {
        var wideFileName = new WString(lpFileName);
        var fileSizeHigh = new IntByReference();
        int ret = JnaStaticKernel32Library.GetCompressedFileSizeW(wideFileName, fileSizeHigh);
        lpFileSizeHigh.accept(fileSizeHigh.getValue());
        return ret;
    }
}
