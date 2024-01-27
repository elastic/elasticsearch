/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import com.sun.jna.IntegerType;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;

import java.util.Arrays;
import java.util.List;

/**
 * Library for Windows/Kernel32
 */
final class JNAKernel32Library {

    private static final Logger logger = LogManager.getLogger(JNAKernel32Library.class);

    // Native library instance must be kept around for the same reason.
    private static final class Holder {
        private static final JNAKernel32Library instance = new JNAKernel32Library();
    }

    private JNAKernel32Library() {
        if (Constants.WINDOWS) {
            try {
                Native.register("kernel32");
                logger.debug("windows/Kernel32 library loaded");
            } catch (NoClassDefFoundError e) {
                logger.warn("JNA not found. native methods and handlers will be disabled.");
            } catch (UnsatisfiedLinkError e) {
                logger.warn("unable to link Windows/Kernel32 library. native methods and handlers will be disabled.");
            }
        }
    }

    static JNAKernel32Library getInstance() {
        return Holder.instance;
    }

    public static class SizeT extends IntegerType {

        // JNA requires this no-arg constructor to be public,
        // otherwise it fails to register kernel32 library
        public SizeT() {
            this(0);
        }

        SizeT(long value) {
            super(Native.SIZE_T_SIZE, value);
        }

    }

    /**
     * Retrieves a pseudo handle for the current process.
     *
     * https://msdn.microsoft.com/en-us/library/windows/desktop/ms683179%28v=vs.85%29.aspx
     *
     * @return a pseudo handle to the current process.
     */
    native Pointer GetCurrentProcess();

    /**
     * Closes an open object handle.
     *
     * https://msdn.microsoft.com/en-us/library/windows/desktop/ms724211%28v=vs.85%29.aspx
     *
     * @param handle A valid handle to an open object.
     * @return true if the function succeeds.
     */
    native boolean CloseHandle(Pointer handle);

    /**
     * Creates or opens a new job object
     *
     * https://msdn.microsoft.com/en-us/library/windows/desktop/ms682409%28v=vs.85%29.aspx
     *
     * @param jobAttributes security attributes
     * @param name job name
     * @return job handle if the function succeeds
     */
    native Pointer CreateJobObjectW(Pointer jobAttributes, String name);

    /**
     * Associates a process with an existing job
     *
     * https://msdn.microsoft.com/en-us/library/windows/desktop/ms681949%28v=vs.85%29.aspx
     *
     * @param job job handle
     * @param process process handle
     * @return true if the function succeeds
     */
    native boolean AssignProcessToJobObject(Pointer job, Pointer process);

    /**
     * Basic limit information for a job object
     *
     * https://msdn.microsoft.com/en-us/library/windows/desktop/ms684147%28v=vs.85%29.aspx
     */
    public static class JOBOBJECT_BASIC_LIMIT_INFORMATION extends Structure implements Structure.ByReference {
        public long PerProcessUserTimeLimit;
        public long PerJobUserTimeLimit;
        public int LimitFlags;
        public SizeT MinimumWorkingSetSize;
        public SizeT MaximumWorkingSetSize;
        public int ActiveProcessLimit;
        public Pointer Affinity;
        public int PriorityClass;
        public int SchedulingClass;

        @Override
        protected List<String> getFieldOrder() {
            return Arrays.asList(
                "PerProcessUserTimeLimit",
                "PerJobUserTimeLimit",
                "LimitFlags",
                "MinimumWorkingSetSize",
                "MaximumWorkingSetSize",
                "ActiveProcessLimit",
                "Affinity",
                "PriorityClass",
                "SchedulingClass"
            );
        }
    }

    /**
     * Constant for JOBOBJECT_BASIC_LIMIT_INFORMATION in Query/Set InformationJobObject
     */
    static final int JOBOBJECT_BASIC_LIMIT_INFORMATION_CLASS = 2;

    /**
     * Constant for LimitFlags, indicating a process limit has been set
     */
    static final int JOB_OBJECT_LIMIT_ACTIVE_PROCESS = 8;

    /**
     * Get job limit and state information
     *
     * https://msdn.microsoft.com/en-us/library/windows/desktop/ms684925%28v=vs.85%29.aspx
     *
     * @param job job handle
     * @param infoClass information class constant
     * @param info pointer to information structure
     * @param infoLength size of information structure
     * @param returnLength length of data written back to structure (or null if not wanted)
     * @return true if the function succeeds
     */
    native boolean QueryInformationJobObject(Pointer job, int infoClass, Pointer info, int infoLength, Pointer returnLength);

    /**
     * Set job limit and state information
     *
     * https://msdn.microsoft.com/en-us/library/windows/desktop/ms686216%28v=vs.85%29.aspx
     *
     * @param job job handle
     * @param infoClass information class constant
     * @param info pointer to information structure
     * @param infoLength size of information structure
     * @return true if the function succeeds
     */
    native boolean SetInformationJobObject(Pointer job, int infoClass, Pointer info, int infoLength);
}
