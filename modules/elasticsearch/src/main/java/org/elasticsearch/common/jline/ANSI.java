/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.common.jline;

import jline.ANSIBuffer;
import jline.Terminal;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

/**
 * @author kimchy (Shay Banon)
 */
public class ANSI {
    //
    // Detection/Enabled Muck
    //

    /**
     * Tries to detect if the current system supports ANSI.
     */
    private static boolean detect() {
        if (System.getProperty("jline.enabled", "false").equalsIgnoreCase("false")) {
            return false;
        }
        boolean enabled = Terminal.getTerminal().isANSISupported();

        if (!enabled) {
            String force = System.getProperty(ANSI.class.getName() + ".force", "false");
            enabled = Boolean.valueOf(force).booleanValue();
        }

        return enabled;
    }

    public static boolean isDetected() {
        return detect();
    }

    private static Boolean enabled;

    public static void setEnabled(final boolean flag) {
        enabled = Boolean.valueOf(flag);
    }

    public static boolean isEnabled() {
        if (enabled == null) {
            enabled = Boolean.valueOf(isDetected());
        }

        return enabled.booleanValue();
    }

    //
    // Code
    //

    public static class Code {
        //
        // NOTE: Some fields duplicated from jline.ANSIBuffer.ANSICodes to change access modifiers
        //

        public static final int OFF = 0;
        public static final int BOLD = 1;
        public static final int UNDERSCORE = 4;
        public static final int BLINK = 5;
        public static final int REVERSE = 7;
        public static final int CONCEALED = 8;

        public static final int FG_BLACK = 30;
        public static final int FG_RED = 31;
        public static final int FG_GREEN = 32;
        public static final int FG_YELLOW = 33;
        public static final int FG_BLUE = 34;
        public static final int FG_MAGENTA = 35;
        public static final int FG_CYAN = 36;
        public static final int FG_WHITE = 37;

        public static final int BLACK = FG_BLACK;
        public static final int RED = FG_RED;
        public static final int GREEN = FG_GREEN;
        public static final int YELLOW = FG_YELLOW;
        public static final int BLUE = FG_BLUE;
        public static final int MAGENTA = FG_MAGENTA;
        public static final int CYAN = FG_CYAN;
        public static final int WHITE = FG_WHITE;

        public static final int BG_BLACK = 40;
        public static final int BG_RED = 41;
        public static final int BG_GREEN = 42;
        public static final int BG_YELLOW = 43;
        public static final int BG_BLUE = 44;
        public static final int BG_MAGENTA = 45;
        public static final int BG_CYAN = 46;
        public static final int BG_WHITE = 47;

        /**
         * A map of code names to values.
         */
        private static final Map NAMES_TO_CODES;

        /**
         * A map of codes to name.
         */
        private static final Map CODES_TO_NAMES;

        static {
            Field[] fields = Code.class.getDeclaredFields();
            Map names = new HashMap(fields.length);
            Map codes = new HashMap(fields.length);

            try {
                for (int i = 0; i < fields.length; i++) {
                    // Skip anything non-public, all public fields are codes
                    int mods = fields[i].getModifiers();
                    if (!Modifier.isPublic(mods)) {
                        continue;
                    }

                    String name = fields[i].getName();
                    Number code = (Number) fields[i].get(Code.class);

                    names.put(name, code);
                    codes.put(code, name);
                }
            }
            catch (IllegalAccessException e) {
                // This should never happen
                throw new Error(e);
            }

            NAMES_TO_CODES = names;
            CODES_TO_NAMES = codes;
        }

        /**
         * Returns the ANSI code for the given symbolic name.  Supported symbolic names are all defined as
         * fields in {@link ANSI.Code} where the case is not significant.
         */
        public static int forName(final String name) throws IllegalArgumentException {
            assert name != null;

            // All names in the map are upper-case
            String tmp = name.toUpperCase();
            Number code = (Number) NAMES_TO_CODES.get(tmp);

            if (code == null) {
                throw new IllegalArgumentException("Invalid ANSI code name: " + name);
            }

            return code.intValue();
        }

        /**
         * Returns the symbolic name for the given ANSI code.
         */
        public static String name(final int code) throws IllegalArgumentException {
            String name = (String) CODES_TO_NAMES.get(Integer.valueOf(code));

            if (name == null) {
                throw new IllegalArgumentException("Invalid ANSI code: " + code);
            }

            return name;
        }
    }

    //
    // Buffer
    //

    public static class Buffer {
        private final StringBuffer buff = new StringBuffer();

        public final boolean autoClear = true;

        public String toString() {
            try {
                return buff.toString();
            }
            finally {
                if (autoClear) clear();
            }
        }

        public void clear() {
            buff.setLength(0);
        }

        public int size() {
            return buff.length();
        }

        public Buffer append(final String text) {
            buff.append(text);

            return this;
        }

        public Buffer append(final Object obj) {
            return append(String.valueOf(obj));
        }

        public Buffer attrib(final int code) {
            if (isEnabled()) {
                buff.append(ANSIBuffer.ANSICodes.attrib(code));
            }

            return this;
        }

        public Buffer attrib(final String text, final int code) {
            assert text != null;

            if (isEnabled()) {
                buff.append(ANSIBuffer.ANSICodes.attrib(code)).append(text).append(ANSIBuffer.ANSICodes.attrib(Code.OFF));
            } else {
                buff.append(text);
            }

            return this;
        }

        public Buffer attrib(final String text, final String codeName) {
            return attrib(text, Code.forName(codeName));
        }
    }

    //
    // Renderer
    //

    public static class Renderer {
        public static final String BEGIN_TOKEN = "@|";

        private static final int BEGIN_TOKEN_SIZE = BEGIN_TOKEN.length();

        public static final String END_TOKEN = "|";

        private static final int END_TOKEN_SIZE = END_TOKEN.length();

        public static final String CODE_TEXT_SEPARATOR = " ";

        public static final String CODE_LIST_SEPARATOR = ",";

        private final Buffer buff = new Buffer();

        public String render(final String input) throws RenderException {
            assert input != null;

            // current, prefix and suffix positions
            int c = 0, p, s;

            while (c < input.length()) {
                p = input.indexOf(BEGIN_TOKEN, c);
                if (p < 0) {
                    break;
                }

                s = input.indexOf(END_TOKEN, p + BEGIN_TOKEN_SIZE);
                if (s < 0) {
                    throw new RenderException("Missing '" + END_TOKEN + "': " + input);
                }

                String expr = input.substring(p + BEGIN_TOKEN_SIZE, s);

                buff.append(input.substring(c, p));

                evaluate(expr);

                c = s + END_TOKEN_SIZE;
            }

            buff.append(input.substring(c));

            return buff.toString();
        }

        private void evaluate(final String input) throws RenderException {
            assert input != null;

            int i = input.indexOf(CODE_TEXT_SEPARATOR);
            if (i < 0) {
                throw new RenderException("Missing ANSI code/text separator '" + CODE_TEXT_SEPARATOR + "': " + input);
            }

            String tmp = input.substring(0, i);
            String[] codes = tmp.split(CODE_LIST_SEPARATOR);
            String text = input.substring(i + 1, input.length());

            for (int j = 0; j < codes.length; j++) {
                int code = Code.forName(codes[j]);
                buff.attrib(code);
            }

            buff.append(text);

            buff.attrib(Code.OFF);
        }

        //
        // RenderException
        //

        public static class RenderException
                extends RuntimeException {
            public RenderException(final String msg) {
                super(msg);
            }
        }

        //
        // Helpers
        //

        public static boolean test(final String text) {
            return text != null && text.indexOf(BEGIN_TOKEN) >= 0;
        }

        public static String encode(final String text, final int code) {
            return new StringBuffer(BEGIN_TOKEN).
                    append(Code.name(code)).
                    append(CODE_TEXT_SEPARATOR).
                    append(text).
                    append(END_TOKEN).
                    toString();
        }
    }

    //
    // RenderWriter
    //

    public static class RenderWriter extends PrintWriter {
        private final Renderer renderer = new Renderer();

        public RenderWriter(final OutputStream out) {
            super(out);
        }

        public RenderWriter(final OutputStream out, final boolean autoFlush) {
            super(out, autoFlush);
        }

        public RenderWriter(final Writer out) {
            super(out);
        }

        public RenderWriter(final Writer out, final boolean autoFlush) {
            super(out, autoFlush);
        }

        public void write(final String s) {
            if (Renderer.test(s)) {
                super.write(renderer.render(s));
            } else {
                super.write(s);
            }
        }
    }
}
