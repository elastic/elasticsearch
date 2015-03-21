/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.jna;

import com.google.common.collect.ImmutableList;
import com.sun.jna.Native;
import com.sun.jna.win32.StdCallLibrary;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.ArrayList;
import java.util.List;


/**
 * Library for Windows/Kernel32
 */
public class Kernel32Library {

    private static ESLogger logger = Loggers.getLogger(Kernel32Library.class);

    // Callbacks must be kept around in order to be able to be called later,
    // when the Windows ConsoleCtrlHandler sends an event.
    private List<NativeHandlerCallback> callbacks = new ArrayList<>();

    // Native library instance must be kept around for the same reason.
    private final static class Holder {
        private final static Kernel32Library instance = new Kernel32Library();
    }

    private Kernel32Library() {
        try {
            Native.register("kernel32");
            logger.debug("windows/Kernel32 library loaded");
        } catch (NoClassDefFoundError e) {
            logger.warn("JNA not found. native methods and handlers will be disabled.");
        } catch (UnsatisfiedLinkError e) {
            logger.warn("unable to link Windows/Kernel32 library. native methods and handlers will be disabled.");
        }
    }

    public static Kernel32Library getInstance() {
        return Holder.instance;
    }

    /**
     * Adds a Console Ctrl Handler.
     *
     * @param handler
     * @return true if the handler is correctly set
     * @throws java.lang.UnsatisfiedLinkError if the Kernel32 library is not loaded or if the native function is not found
     * @throws java.lang.NoClassDefFoundError if the library for native calls is missing
     */
    public boolean addConsoleCtrlHandler(ConsoleCtrlHandler handler) {
        boolean result = false;
        if (handler != null) {
            NativeHandlerCallback callback = new NativeHandlerCallback(handler);
            result = SetConsoleCtrlHandler(callback, true);
            if (result) {
                callbacks.add(callback);
            }
        }
        return result;
    }

    public ImmutableList<Object> getCallbacks() {
        return ImmutableList.builder().addAll(callbacks).build();
    }

    /**
     * Native call to the Kernel32 API to set a new Console Ctrl Handler.
     *
     * @param handler
     * @param add
     * @return true if the handler is correctly set
     * @throws java.lang.UnsatisfiedLinkError if the Kernel32 library is not loaded or if the native function is not found
     * @throws java.lang.NoClassDefFoundError if the library for native calls is missing
     */
    public native boolean SetConsoleCtrlHandler(StdCallLibrary.StdCallCallback handler, boolean add);

    /**
     * Handles consoles event with WIN API
     * <p/>
     * See http://msdn.microsoft.com/en-us/library/windows/desktop/ms683242%28v=vs.85%29.aspx
     */
    class NativeHandlerCallback implements StdCallLibrary.StdCallCallback {

        private final ConsoleCtrlHandler handler;

        public NativeHandlerCallback(ConsoleCtrlHandler handler) {
            this.handler = handler;
        }

        public boolean callback(long dwCtrlType) {
            int event = (int) dwCtrlType;
            if (logger.isDebugEnabled()) {
                logger.debug("console control handler receives event [{}@{}]", event, dwCtrlType);

            }
            return handler.handle(event);
        }
    }

    public interface ConsoleCtrlHandler {

        public static final int CTRL_CLOSE_EVENT = 2;

        /**
         * Handles the Ctrl event.
         *
         * @param code the code corresponding to the Ctrl sent.
         * @return true if the handler processed the event, false otherwise. If false, the next handler will be called.
         */
        boolean handle(int code);
    }
}
