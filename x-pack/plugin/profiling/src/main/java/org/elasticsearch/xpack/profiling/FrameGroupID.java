/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import org.elasticsearch.core.SuppressForbidden;

import java.io.File;
import java.util.Objects;

public class FrameGroupID {
    private static final String UNIX_PATH_SEPARATOR = "/";

    @SuppressForbidden(reason = "Using pathSeparator constant to extract the filename with low overhead")
    private static String getFilename(String fullPath) {
        if (fullPath == null || fullPath.isEmpty()) {
            return fullPath;
        }
        int lastSeparatorIdx = fullPath.lastIndexOf(UNIX_PATH_SEPARATOR);
        return lastSeparatorIdx == -1 ? fullPath : fullPath.substring(lastSeparatorIdx + 1);
    }

    public static String create(
        String fileId,
        Integer addressOrLine,
        String exeFilename,
        String sourceFilename,
        String functionName
    ) {
        StringBuilder sb = new StringBuilder();
        if (functionName.isEmpty()) {
            sb.append(fileId);
            sb.append(addressOrLine);
        } else {
            sb.append(exeFilename);
            sb.append(functionName);
            sb.append(getFilename(sourceFilename));
        }
        return sb.toString();
    }
}
