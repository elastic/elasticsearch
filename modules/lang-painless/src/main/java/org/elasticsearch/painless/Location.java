/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
    public static String computeSourceName(String scriptName) {
        StringBuilder fileName = new StringBuilder();
        // its an anonymous script, include at least a portion of the source to help identify which one it is
        // but don't create stacktraces with filenames that contain newlines or huge names.

        // truncate to the first newline
        int limit = scriptName.indexOf('\n');
        if (limit >= 0) {
            int limit2 = scriptName.indexOf('\r');
            if (limit2 >= 0) {
                limit = Math.min(limit, limit2);
            }
        } else {
            limit = scriptName.length();
        }

        // truncate to our limit
        limit = Math.min(limit, MAX_NAME_LENGTH);
        fileName.append(scriptName, 0, limit);

        // if we truncated, make it obvious
        if (limit != scriptName.length()) {
            fileName.append(" ...");
        }
        return fileName.toString();
    }
}
