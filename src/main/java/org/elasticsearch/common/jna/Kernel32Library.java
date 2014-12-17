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
import com.sun.jna.Library;
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

    private Kernel32 internal;

    private List<NativeHandlerCallback> callbacks = new ArrayList<>();

    private final static class Holder {
        private final static Kernel32Library instance = new Kernel32Library();
    }

    private Kernel32Library() {
        try {
            internal = (Kernel32)Native.synchronizedLibrary((Kernel32)Native.loadLibrary("kernel32", Kernel32.class));
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

    public boolean addConsoleCtrlHandler(ConsoleCtrlHandler handler) {
        if (internal == null) {
            throw new UnsupportedOperationException("windows/Kernel32 library not loaded, console ctrl handler cannot be set");
        }
        boolean result = false;
        if (handler != null) {
            NativeHandlerCallback callback = new NativeHandlerCallback(handler);
            result = internal.SetConsoleCtrlHandler(callback, true);
            if (result) {
                callbacks.add(callback);
            }
        }
        return result;
    }

    public ImmutableList<Object> getCallbacks() {
        return ImmutableList.builder().addAll(callbacks).build();
    }

    interface Kernel32 extends Library {

        /**
         * Registers a Console Ctrl Handler.
         *
         * @param handler
         * @param add
         * @return true if the handler is correctly set
         */
        public boolean SetConsoleCtrlHandler(StdCallLibrary.StdCallCallback handler, boolean add);
    }

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
