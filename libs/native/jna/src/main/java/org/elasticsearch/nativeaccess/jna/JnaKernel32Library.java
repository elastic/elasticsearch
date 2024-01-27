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

import org.elasticsearch.nativeaccess.NativeAccess.ConsoleCtrlHandler;
import org.elasticsearch.nativeaccess.jna.JnaStaticKernel32Library.JnaJobObjectBasicLimitInformation;
import org.elasticsearch.nativeaccess.jna.JnaStaticKernel32Library.JnaMemoryBasicInformation;
import org.elasticsearch.nativeaccess.jna.JnaStaticKernel32Library.NativeHandlerCallback;
import org.elasticsearch.nativeaccess.jna.JnaStaticKernel32Library.SizeT;
import org.elasticsearch.nativeaccess.lib.Kernel32Library;

import java.util.function.IntConsumer;

class JnaKernel32Library implements Kernel32Library {

    private static class JnaHandle implements Handle {
        final Pointer pointer;

        JnaHandle(Pointer pointer) {
            this.pointer = pointer;
        }
    }

    static class JnaAddress implements Address {
        final Pointer pointer;

        JnaAddress(Pointer pointer) {
            this.pointer = pointer;
        }


        @Override
        public Address add(long offset) {
            return new JnaAddress(new Pointer(Pointer.nativeValue(pointer) + offset));
        }
    }

    @Override
    public Handle GetCurrentProcess() {
        return new JnaHandle(JnaStaticKernel32Library.GetCurrentProcess());
    }

    @Override
    public boolean CloseHandle(Handle handle) {
        assert handle instanceof JnaHandle;
        var jnaHandle = (JnaHandle) handle;
        return JnaStaticKernel32Library.CloseHandle(jnaHandle.pointer);
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
    public boolean VirtualLock(Address address, long size) {
        assert address instanceof JnaAddress;
        var jnaAddress = (JnaAddress) address;
        return JnaStaticKernel32Library.VirtualLock(jnaAddress.pointer, new SizeT(size));
    }

    @Override
    public int VirtualQueryEx(Handle handle, Address address, MemoryBasicInformation memoryInfo) {
        assert handle instanceof JnaHandle;
        assert address instanceof JnaAddress;
        assert memoryInfo instanceof JnaMemoryBasicInformation;
        var jnaHandle = (JnaHandle) handle;
        var jnaAddress = (JnaAddress) address;
        var jnaMemoryInfo = (JnaMemoryBasicInformation) memoryInfo;
        return JnaStaticKernel32Library.VirtualQueryEx(
            jnaHandle.pointer,
            jnaAddress.pointer,
            jnaMemoryInfo,
            jnaMemoryInfo.size());
    }

    @Override
    public boolean SetProcessWorkingSetSize(Handle handle, long minSize, long maxSize) {
        assert handle instanceof JnaHandle;
        var jnaHandle = (JnaHandle) handle;
        return JnaStaticKernel32Library.SetProcessWorkingSetSize(jnaHandle.pointer, new SizeT(minSize), new SizeT(maxSize));
    }

    @Override
    public int GetCompressedFileSizeW(String lpFileName, IntConsumer lpFileSizeHigh) {
        var wideFileName = new WString(lpFileName);
        var fileSizeHigh = new IntByReference();
        int ret = JnaStaticKernel32Library.GetCompressedFileSizeW(wideFileName, fileSizeHigh);
        lpFileSizeHigh.accept(fileSizeHigh.getValue());
        return ret;
    }

    @Override
    public int GetShortPathNameW(String lpszLongPath, char[] lpszShortPath, int cchBuffer) {
        var wideFileName = new WString(lpszLongPath);
        return JnaStaticKernel32Library.GetShortPathNameW(wideFileName, lpszShortPath, cchBuffer);
    }

    @Override
    public boolean SetConsoleCtrlHandler(ConsoleCtrlHandler handler, boolean add) {
        NativeHandlerCallback callback = new NativeHandlerCallback(handler);
        return JnaStaticKernel32Library.SetConsoleCtrlHandler(callback, true);
    }

    @Override
    public Handle CreateJobObjectW() {
        return new JnaHandle(JnaStaticKernel32Library.CreateJobObjectW(null, null));
    }

    @Override
    public boolean AssignProcessToJobObject(Handle job, Handle process) {
        assert job instanceof JnaHandle;
        assert process instanceof JnaHandle;
        var jnaJob = (JnaHandle) job;
        var jnaProcess = (JnaHandle) process;
        return JnaStaticKernel32Library.AssignProcessToJobObject(jnaJob.pointer, jnaProcess.pointer);
    }

    @Override
    public JobObjectBasicLimitInformation newJobObjectBasicLimitInformation() {
        return new JnaJobObjectBasicLimitInformation();
    }

    @Override
    public boolean QueryInformationJobObject(Handle job, int infoClass, JobObjectBasicLimitInformation info) {
        assert job instanceof JnaHandle;
        assert info instanceof JnaJobObjectBasicLimitInformation;
        var jnaJob = (JnaHandle) job;
        var jnaInfo = (JnaJobObjectBasicLimitInformation) info;
        return JnaStaticKernel32Library.QueryInformationJobObject(jnaJob.pointer, infoClass, jnaInfo, jnaInfo.size(), null);
    }

    @Override
    public boolean SetInformationJobObject(Handle job, int infoClass, JobObjectBasicLimitInformation info) {
        assert job instanceof JnaHandle;
        assert info instanceof JnaJobObjectBasicLimitInformation;
        var jnaJob = (JnaHandle) job;
        var jnaInfo = (JnaJobObjectBasicLimitInformation) info;
        return JnaStaticKernel32Library.SetInformationJobObject(jnaJob.pointer, infoClass, jnaInfo, jnaInfo.size());
    }
}
