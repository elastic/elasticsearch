/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy.entitlements;

import org.elasticsearch.entitlement.runtime.policy.ExternalEntitlement;
import org.elasticsearch.entitlement.runtime.policy.PolicyValidationException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Describes a file entitlement with a path and mode.
 */
public record FilesEntitlement(List<FileData> filesData) implements Entitlement {

    public static final FilesEntitlement EMPTY = new FilesEntitlement(List.of());

    public enum Mode {
        READ,
        READ_WRITE
    }

    public enum BaseDir {
        NONE,
        CONFIG,
        DATA,
        TEMP
    }

    public record FileData(String path, Mode mode, BaseDir baseDir) {

    }

    private static Mode parseMode(String mode) {
        if (mode.equals("read")) {
            return Mode.READ;
        } else if (mode.equals("read_write")) {
            return Mode.READ_WRITE;
        } else {
            throw new PolicyValidationException("invalid mode: " + mode + ", valid values: [read, read_write]");
        }
    }

    private static BaseDir parseBaseDir(String baseDir) {
        return switch (baseDir) {
            case "config" -> BaseDir.CONFIG;
            case "data" -> BaseDir.DATA;
            case "temp" -> BaseDir.TEMP;
            default -> throw new PolicyValidationException("invalid base_dir: " + baseDir + ", valid values: [config, data, temp]");
        };
    }

    @ExternalEntitlement(parameterNames = { "paths" }, esModulesOnly = false)
    @SuppressWarnings("unchecked")
    public static FilesEntitlement build(List<Object> paths) {
        if (paths == null || paths.isEmpty()) {
            throw new PolicyValidationException("must specify at least one path");
        }
        List<FileData> filesData = new ArrayList<>();
        for (Object object : paths) {
            Map<String, String> file = new HashMap<>((Map<String, String>) object);
            String path = file.remove("path");
            if (path == null) {
                throw new PolicyValidationException("files entitlement must contain path for every listed file");
            }
            String mode = file.remove("mode");
            if (mode == null) {
                throw new PolicyValidationException("files entitlement must contain mode for every listed file");
            }
            String baseDirString = file.remove("base_dir");
            final BaseDir baseDir = baseDirString == null ? BaseDir.NONE : parseBaseDir(baseDirString);

            if (file.isEmpty() == false) {
                throw new PolicyValidationException("unknown key(s) " + file + " in a listed file for files entitlement");
            }
            filesData.add(new FileData(path, parseMode(mode), baseDir));
        }
        return new FilesEntitlement(filesData);
    }
}
