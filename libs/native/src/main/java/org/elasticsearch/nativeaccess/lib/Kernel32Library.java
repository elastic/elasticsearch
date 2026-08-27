/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess.lib;

import org.elasticsearch.foreign.Addressable;
import org.elasticsearch.foreign.CaptureSystemError;
import org.elasticsearch.foreign.Function;
import org.elasticsearch.foreign.LibrarySpecification;
import org.elasticsearch.foreign.LinkerHelper;
import org.elasticsearch.foreign.Offset;
import org.elasticsearch.foreign.Platform;
import org.elasticsearch.foreign.Sizeof;
import org.elasticsearch.foreign.StructFactory;
import org.elasticsearch.foreign.StructSize;
import org.elasticsearch.foreign.StructSpecification;
import org.elasticsearch.foreign.WideString;
import org.elasticsearch.nativeaccess.WindowsNativeAccess.ConsoleCtrlHandler;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.function.IntConsumer;

import static java.lang.foreign.ValueLayout.JAVA_CHAR;
import static java.lang.foreign.ValueLayout.JAVA_INT;

@LibrarySpecification(
    name = "kernel32",
    system = true,
    unavailableOn = { Platform.LINUX_X64, Platform.LINUX_AARCH64, Platform.DARWIN_X64, Platform.DARWIN_AARCH64 }
)
public abstract class Kernel32Library {

    /** Opaque wrapper for a Win32 {@code HANDLE}, giving call sites a distinct type from raw pointers. */
    public record Handle(MemorySegment segment) implements Addressable {}

    /**
     * Opaque wrapper for a native pointer. {@link #add} is the only place pointer arithmetic is
     * performed on the wrapped address.
     */
    public record Address(MemorySegment segment) implements Addressable {
        public Address add(long offset) {
            return new Address(MemorySegment.ofAddress(segment.address() + offset));
        }
    }

    @Function("GetCurrentProcess")
    protected abstract MemorySegment GetCurrentProcessRaw();

    public Handle GetCurrentProcess() {
        return new Handle(GetCurrentProcessRaw());
    }

    @CaptureSystemError
    @Function("CloseHandle")
    public abstract boolean CloseHandle(Handle handle);

    public int GetLastError() {
        return LinkerHelper.systemError();
    }

    /**
     * Contains information about a range of pages in the virtual address space of a process.
     * The VirtualQuery and VirtualQueryEx functions use this structure.
     *
     * @see <a href="https://msdn.microsoft.com/en-us/library/windows/desktop/aa366775%28v=vs.85%29.aspx">MemoryBasicInformation docs</a>
     */
    @StructSpecification(sparse = true)
    @StructSize(56)
    // TODO: MSDN defines State/Protect/Type as 4-byte DWORDs (48-byte struct); the pre-migration
    // JdkKernel32Library read them as 8-byte longs (56-byte struct). Preserved verbatim for
    // migration parity; revisit in a follow-up.
    public interface MemoryBasicInformation {
        @Sizeof
        int sizeof();

        @Offset(0)
        MemorySegment BaseAddress();

        @Offset(24)
        long RegionSize();

        @Offset(32)
        long State();

        @Offset(40)
        long Protect();

        @Offset(48)
        long Type();

        /** Wraps the base-address pointer as an {@link Address}. */
        default Address baseAddress() {
            return new Address(BaseAddress());
        }
    }

    /**
     * Create a new MemoryBasicInformation for use by VirtualQuery and VirtualQueryEx
     */
    @StructFactory
    public abstract MemoryBasicInformation newMemoryBasicInformation();

    /**
     * Locks the specified region of the process's virtual address space into physical
     * memory, ensuring that subsequent access to the region will not incur a page fault.
     *
     * @param address A pointer to the base address of the region of pages to be locked.
     * @param size The size of the region to be locked, in bytes.
     * @return true if the function succeeds
     * @see <a href="https://msdn.microsoft.com/en-us/library/windows/desktop/aa366895%28v=vs.85%29.aspx">VirtualLock docs</a>
     */
    @CaptureSystemError
    @Function("VirtualLock")
    public abstract boolean VirtualLock(Address address, long size);

    /** Raw binding for VirtualQueryEx; use {@link #VirtualQueryEx(Handle, Address, MemoryBasicInformation)} instead. */
    @CaptureSystemError
    @Function("VirtualQueryEx")
    protected abstract int VirtualQueryEx(Handle process, Address address, MemoryBasicInformation memoryInfo, long dwLength);

    /**
     * Retrieves information about a range of pages within the virtual address space of a specified process.
     *
     * @param process A handle to the process whose memory information is queried.
     * @param address A pointer to the base address of the region of pages to be queried.
     * @param memoryInfo A pointer to a structure in which information about the specified page range is returned.
     * @return the actual number of bytes returned in the information buffer.
     * @see <a href="https://msdn.microsoft.com/en-us/library/windows/desktop/aa366907%28v=vs.85%29.aspx">VirtualQueryEx docs</a>
     */
    public int VirtualQueryEx(Handle process, Address address, MemoryBasicInformation memoryInfo) {
        return VirtualQueryEx(process, address, memoryInfo, memoryInfo.sizeof());
    }

    /**
     * Sets the minimum and maximum working set sizes for the specified process.
     *
     * @param handle A handle to the process whose working set sizes is to be set.
     * @param minSize The minimum working set size for the process, in bytes.
     * @param maxSize The maximum working set size for the process, in bytes.
     * @return true if the function succeeds.
     * @see <a href="https://msdn.microsoft.com/en-us/library/windows/desktop/ms686234%28v=vs.85%29.aspx">SetProcessWorkingSetSize docs</a>
     */
    @CaptureSystemError
    @Function("SetProcessWorkingSetSize")
    public abstract boolean SetProcessWorkingSetSize(Handle handle, long minSize, long maxSize);

    /** Raw binding for GetCompressedFileSizeW; use {@link #GetCompressedFileSizeW(String, IntConsumer)} instead. */
    @CaptureSystemError
    @Function("GetCompressedFileSizeW")
    protected abstract int GetCompressedFileSizeW(@WideString String fileName, MemorySegment fileSizeHighOut);

    /**
     * Retrieves the actual number of bytes of disk storage used to store a specified file.
     *
     * https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getcompressedfilesizew
     *
     * @param lpFileName the path string
     * @param lpFileSizeHigh pointer to high-order DWORD for compressed file size (or null if not needed)
     * @return the low-order DWORD for compressed file size
     */
    public int GetCompressedFileSizeW(String lpFileName, IntConsumer lpFileSizeHigh) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment fileSizeHigh = arena.allocate(JAVA_INT);
            int ret = GetCompressedFileSizeW(lpFileName, fileSizeHigh);
            lpFileSizeHigh.accept(fileSizeHigh.get(JAVA_INT, 0));
            return ret;
        }
    }

    /** Raw binding for GetShortPathNameW; use {@link #GetShortPathNameW(String, char[], int)} instead. */
    @CaptureSystemError
    @Function("GetShortPathNameW")
    protected abstract int GetShortPathNameW(@WideString String longPath, MemorySegment shortPathOut, int cchBuffer);

    /**
     * Retrieves the short path form of the specified path.
     *
     * @param lpszLongPath  the path string
     * @param lpszShortPath a buffer to receive the short name
     * @param cchBuffer     the size of the buffer
     * @return the length of the string copied into {@code lpszShortPath}, otherwise zero for failure
     * @see <a href="https://msdn.microsoft.com/en-us/library/windows/desktop/aa364989.aspx">GetShortPathName docs</a>
     */
    public int GetShortPathNameW(String lpszLongPath, char[] lpszShortPath, int cchBuffer) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment shortPath = lpszShortPath != null ? arena.allocate(JAVA_CHAR, cchBuffer) : MemorySegment.NULL;

            int ret = GetShortPathNameW(lpszLongPath, shortPath, cchBuffer);
            if (shortPath != MemorySegment.NULL) {
                for (int i = 0; i < cchBuffer; ++i) {
                    lpszShortPath[i] = shortPath.getAtIndex(JAVA_CHAR, i);
                }
            }
            return ret;
        }
    }

    /**
     * Native call to the Kernel32 API to set a new Console Ctrl Handler.
     *
     * @param handler A callback to handle control events
     * @param add     True if the handler should be added, false if it should replace existing handlers
     * @return true if the handler is correctly set
     * @see <a href="https://learn.microsoft.com/en-us/windows/console/setconsolectrlhandler">SetConsoleCtrlHandler docs</a>
     */
    @CaptureSystemError
    @Function("SetConsoleCtrlHandler")
    public abstract boolean SetConsoleCtrlHandler(ConsoleCtrlHandler handler, boolean add);

    /** Raw binding for CreateJobObjectW; use {@link #CreateJobObjectW()} instead. */
    @CaptureSystemError
    @Function("CreateJobObjectW")
    protected abstract MemorySegment CreateJobObjectWRaw(MemorySegment lpJobAttributes, MemorySegment lpName);

    /**
     * Creates or opens a new job object
     *
     * https://msdn.microsoft.com/en-us/library/windows/desktop/ms682409%28v=vs.85%29.aspx
     * Note: the two params to the underlying API are omitted because all implementations pass null for them both
     *
     * @return job handle if the function succeeds
     */
    public Handle CreateJobObjectW() {
        return new Handle(CreateJobObjectWRaw(MemorySegment.NULL, MemorySegment.NULL));
    }

    /**
     * Associates a process with an existing job
     *
     * https://msdn.microsoft.com/en-us/library/windows/desktop/ms681949%28v=vs.85%29.aspx
     *
     * @param job job handle
     * @param process process handle
     * @return true if the function succeeds
     */
    @CaptureSystemError
    @Function("AssignProcessToJobObject")
    public abstract boolean AssignProcessToJobObject(Handle job, Handle process);

    /**
     * Basic limit information for a job object
     *
     * https://msdn.microsoft.com/en-us/library/windows/desktop/ms684147%28v=vs.85%29.aspx
     */
    @StructSpecification(sparse = true)
    @StructSize(64)
    public interface JobObjectBasicLimitInformation {
        @Sizeof
        int sizeof();

        @Offset(16)
        void setLimitFlags(int v);

        @Offset(40)
        void setActiveProcessLimit(int v);
    }

    @StructFactory
    public abstract JobObjectBasicLimitInformation newJobObjectBasicLimitInformation();

    /**
     * Raw binding for QueryInformationJobObject; use
     * {@link #QueryInformationJobObject(Handle, int, JobObjectBasicLimitInformation)} instead.
     */
    @CaptureSystemError
    @Function("QueryInformationJobObject")
    protected abstract boolean QueryInformationJobObject(
        Handle job,
        int infoClass,
        JobObjectBasicLimitInformation info,
        int cbInfoLength,
        MemorySegment returnLength
    );

    /**
     * Get job limit and state information
     *
     * https://msdn.microsoft.com/en-us/library/windows/desktop/ms684925%28v=vs.85%29.aspx
     * Note: The infoLength parameter to the underlying API is omitted because this wrapper computes it from {@code info}
     * Note: The returnLength parameter to the underlying API is omitted because all implementations pass null
     *
     * @param job job handle
     * @param infoClass information class constant
     * @param info pointer to information structure
     * @return true if the function succeeds
     */
    public boolean QueryInformationJobObject(Handle job, int infoClass, JobObjectBasicLimitInformation info) {
        return QueryInformationJobObject(job, infoClass, info, info.sizeof(), MemorySegment.NULL);
    }

    /**
     * Raw binding for SetInformationJobObject; use
     * {@link #SetInformationJobObject(Handle, int, JobObjectBasicLimitInformation)} instead.
     */
    @CaptureSystemError
    @Function("SetInformationJobObject")
    protected abstract boolean SetInformationJobObject(Handle job, int infoClass, JobObjectBasicLimitInformation info, int cbInfoLength);

    /**
     * Set job limit and state information
     *
     * https://msdn.microsoft.com/en-us/library/windows/desktop/ms686216%28v=vs.85%29.aspx
     * Note: The infoLength parameter to the underlying API is omitted because this wrapper computes it from {@code info}
     *
     * @param job job handle
     * @param infoClass information class constant
     * @param info pointer to information structure
     * @return true if the function succeeds
     */
    public boolean SetInformationJobObject(Handle job, int infoClass, JobObjectBasicLimitInformation info) {
        return SetInformationJobObject(job, infoClass, info, info.sizeof());
    }
}
