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

package org.elasticsearch.common.logging;

import org.apache.log4j.Layout;
import org.apache.log4j.WriterAppender;
import org.apache.log4j.helpers.LogLog;
import org.elasticsearch.common.SuppressForbidden;

import java.io.IOException;
import java.io.OutputStream;

/**
 * ConsoleAppender appends log events to <code>System.out</code> or
 * <code>System.err</code> using a layout specified by the user. The
 * default target is <code>System.out</code>.
 * <p>Elasticsearch: Adapter from log4j to allow to disable console logging...</p>
 *
 * @author Ceki G&uuml;lc&uuml;
 * @author Curt Arnold
 * @since 1.1
 */
public class ConsoleAppender extends WriterAppender {

    public static final String SYSTEM_OUT = "System.out";
    public static final String SYSTEM_ERR = "System.err";

    protected String target = SYSTEM_OUT;

    /**
     * Determines if the appender honors reassignments of System.out
     * or System.err made after configuration.
     */
    private boolean follow = true;

    /**
     * Constructs an unconfigured appender.
     */
    public ConsoleAppender() {
    }

    /**
     * Creates a configured appender.
     *
     * @param layout layout, may not be null.
     */
    public ConsoleAppender(Layout layout) {
        this(layout, SYSTEM_OUT);
    }

    /**
     * Creates a configured appender.
     *
     * @param layout layout, may not be null.
     * @param target target, either "System.err" or "System.out".
     */
    public ConsoleAppender(Layout layout, String target) {
        setLayout(layout);
        setTarget(target);
        activateOptions();
    }

    /**
     * Sets the value of the <b>Target</b> option. Recognized values
     * are "System.out" and "System.err". Any other value will be
     * ignored.
     */
    public void setTarget(String value) {
        String v = value.trim();

        if (SYSTEM_OUT.equalsIgnoreCase(v)) {
            target = SYSTEM_OUT;
        } else if (SYSTEM_ERR.equalsIgnoreCase(v)) {
            target = SYSTEM_ERR;
        } else {
            targetWarn(value);
        }
    }

    /**
     * Returns the current value of the <b>Target</b> property. The
     * default value of the option is "System.out".
     * <p>
     * See also {@link #setTarget}.
     */
    public String getTarget() {
        return target;
    }

    /**
     * Sets whether the appender honors reassignments of System.out
     * or System.err made after configuration.
     *
     * @param newValue if true, appender will use value of System.out or
     *                 System.err in force at the time when logging events are appended.
     * @since 1.2.13
     */
    public final void setFollow(final boolean newValue) {
        follow = newValue;
    }

    /**
     * Gets whether the appender honors reassignments of System.out
     * or System.err made after configuration.
     *
     * @return true if appender will use value of System.out or
     *         System.err in force at the time when logging events are appended.
     * @since 1.2.13
     */
    public final boolean getFollow() {
        return follow;
    }

    void targetWarn(String val) {
        LogLog.warn("[" + val + "] should be System.out or System.err.");
        LogLog.warn("Using previously set target, System.out by default.");
    }

    /**
     * Prepares the appender for use.
     */
    @Override
    @SuppressForbidden(reason = "System#out")
    public void activateOptions() {
        if (follow) {
            if (target.equals(SYSTEM_ERR)) {
                setWriter(createWriter(new SystemErrStream()));
            } else {
                setWriter(createWriter(new SystemOutStream()));
            }
        } else {
            if (target.equals(SYSTEM_ERR)) {
                setWriter(createWriter(System.err));
            } else {
                setWriter(createWriter(System.out));
            }
        }

        super.activateOptions();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected
    final void closeWriter() {
        if (follow) {
            super.closeWriter();
        }
    }


    /**
     * An implementation of OutputStream that redirects to the
     * current System.err.
     */
    @SuppressForbidden(reason = "System#err")
    private static class SystemErrStream extends OutputStream {
        public SystemErrStream() {
        }

        @Override
        public void close() {
        }

        @Override
        public void flush() {
            System.err.flush();
        }

        @Override
        public void write(final byte[] b) throws IOException {
            if (!Loggers.consoleLoggingEnabled()) {
                return;
            }
            System.err.write(b);
        }

        @Override

        public void write(final byte[] b, final int off, final int len)
                throws IOException {
            if (!Loggers.consoleLoggingEnabled()) {
                return;
            }
            System.err.write(b, off, len);
        }

        @Override
        public void write(final int b) throws IOException {
            if (!Loggers.consoleLoggingEnabled()) {
                return;
            }
            System.err.write(b);
        }
    }

    /**
     * An implementation of OutputStream that redirects to the
     * current System.out.
     */
    @SuppressForbidden(reason = "System#err")
    private static class SystemOutStream extends OutputStream {
        public SystemOutStream() {
        }

        @Override
        public void close() {
        }

        @Override
        public void flush() {
            System.out.flush();
        }

        @Override
        public void write(final byte[] b) throws IOException {
            if (!Loggers.consoleLoggingEnabled()) {
                return;
            }
            System.out.write(b);
        }

        @Override
        public void write(final byte[] b, final int off, final int len)
                throws IOException {
            if (!Loggers.consoleLoggingEnabled()) {
                return;
            }
            System.out.write(b, off, len);
        }

        @Override
        public void write(final int b) throws IOException {
            if (!Loggers.consoleLoggingEnabled()) {
                return;
            }
            System.out.write(b);
        }
    }

}
