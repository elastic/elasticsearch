/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import org.elasticsearch.common.Strings;

import java.util.Objects;

public final class FrameGroupID {
    private static final char UNIX_PATH_SEPARATOR = '/';

    private FrameGroupID() {}

    public static String getBasenameAndParent(String fullPath) {
        if (Strings.isEmpty(fullPath)) {
            return fullPath;
        }
        int lastSeparatorIdx = fullPath.lastIndexOf(UNIX_PATH_SEPARATOR);
        if (lastSeparatorIdx <= 0) {
            return fullPath;
        }
        int nextSeparatorIdx = fullPath.lastIndexOf(UNIX_PATH_SEPARATOR, lastSeparatorIdx - 1);
        return nextSeparatorIdx == -1 ? fullPath : fullPath.substring(nextSeparatorIdx + 1);
    }

    public static String create(String fileId, Integer addressOrLine, String exeFilename, String sourceFilename, String functionName) {
        if (Strings.isEmpty(functionName)) {
            return Integer.toString(Objects.hash(fileId, addressOrLine));
        }
        if (Strings.isEmpty(sourceFilename)) {
            return Integer.toString(Objects.hash(fileId, functionName));
        }
        return Integer.toString(Objects.hash(exeFilename, functionName, getBasenameAndParent(sourceFilename)));
    }
}
