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

package org.elasticsearch.painless;

import java.util.Objects;

/**
 * Represents a location in script code (name of script + character offset)
 */
public final class Location {
    private final String sourceName;
    private final int offset;
    
    /**
     * Create a new Location 
     * @param sourceName script's name
     * @param offset character offset of script element
     */
    public Location(String sourceName, int offset) {
        this.sourceName = Objects.requireNonNull(sourceName);
        this.offset = offset;
    }
    
    /**
     * Return the script's name
     */
    public String getSourceName() {
        return sourceName;
    }

    /**
     * Return the character offset
     */
    public int getOffset() {
        return offset;
    }

    /**
     * Augments an exception with this location's information.
     */
    public RuntimeException createError(RuntimeException exception) {
        StackTraceElement element = new StackTraceElement(WriterConstants.CLASS_NAME, "compile", sourceName, offset + 1);
        StackTraceElement[] oldStack = exception.getStackTrace();
        StackTraceElement[] newStack = new StackTraceElement[oldStack.length + 1];
        System.arraycopy(oldStack, 0, newStack, 1, oldStack.length);
        newStack[0] = element;
        exception.setStackTrace(newStack);
        assert exception.getStackTrace().length == newStack.length : "non-writeable stacktrace for exception: " + exception.getClass();
        return exception;
    }

    // This maximum length is theoretically 65535 bytes, but as it's CESU-8 encoded we don't know how large it is in bytes, so be safe
    private static final int MAX_NAME_LENGTH = 256;
    
    /** Computes the file name (mostly important for stacktraces) */
    public static String computeSourceName(String scriptName, String source) {
        StringBuilder fileName = new StringBuilder();
        if (scriptName.equals(PainlessScriptEngine.INLINE_NAME)) {
            // its an anonymous script, include at least a portion of the source to help identify which one it is
            // but don't create stacktraces with filenames that contain newlines or huge names.

            // truncate to the first newline
            int limit = source.indexOf('\n');
            if (limit >= 0) {
                int limit2 = source.indexOf('\r');
                if (limit2 >= 0) {
                    limit = Math.min(limit, limit2);
                }
            } else {
                limit = source.length();
            }

            // truncate to our limit
            limit = Math.min(limit, MAX_NAME_LENGTH);
            fileName.append(source, 0, limit);

            // if we truncated, make it obvious
            if (limit != source.length()) {
                fileName.append(" ...");
            }
            fileName.append(" @ <inline script>");
        } else {
            // its a named script, just use the name
            // but don't trust this has a reasonable length!
            if (scriptName.length() > MAX_NAME_LENGTH) {
                fileName.append(scriptName, 0, MAX_NAME_LENGTH);
                fileName.append(" ...");
            } else {
                fileName.append(scriptName);
            }
        }
        return fileName.toString();
    }
}
