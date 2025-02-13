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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
        CONFIG,
        DATA
    }

    public static final class FileData {
        private final Path path;
        private final Mode mode;
        private final Path relativePath;
        private final BaseDir baseDir;

        private FileData(Path path, Mode mode, Path relativePath, BaseDir baseDir) {
            this.path = path;
            this.mode = mode;
            this.relativePath = relativePath;
            this.baseDir = baseDir;
        }

        public static FileData ofPath(Path path, Mode mode) {
            assert path.isAbsolute();
            return new FileData(path, mode, null, null);
        }

        public static FileData ofRelativePath(Path relativePath, BaseDir baseDir, Mode mode) {
            assert relativePath.isAbsolute() == false;
            return new FileData(null, mode, relativePath, baseDir);
        }

        public Path path() {
            return path;
        }

        public Mode mode() {
            return mode;
        }

        public Path relativePath() {
            return relativePath;
        }

        public BaseDir baseDir() {
            return baseDir;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (FileData) obj;
            return Objects.equals(this.path, that.path)
                && Objects.equals(this.mode, that.mode)
                && Objects.equals(this.relativePath, that.relativePath)
                && Objects.equals(this.baseDir, that.baseDir);
        }

        @Override
        public int hashCode() {
            return Objects.hash(path, mode, relativePath, baseDir);
        }
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
        if (baseDir.equals("config")) {
            return BaseDir.CONFIG;
        } else if (baseDir.equals("data")) {
            return BaseDir.DATA;
        }
        throw new PolicyValidationException("invalid relative directory: " + baseDir + ", valid values: [config, data]");
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
            String pathAsString = file.remove("path");
            String relativePathAsString = file.remove("relative_path");
            String relativeTo = file.remove("relative_to");
            String mode = file.remove("mode");

            if (file.isEmpty() == false) {
                throw new PolicyValidationException("unknown key(s) [" + file + "] in a listed file for files entitlement");
            }
            if (mode == null) {
                throw new PolicyValidationException("files entitlement must contain 'mode' for every listed file");
            }
            if ((pathAsString == null && relativePathAsString == null) || (pathAsString != null && relativePathAsString != null)) {
                throw new PolicyValidationException("files entitlement must contain either 'path' or 'relative_path' for every entry");
            }
            if (relativePathAsString != null) {
                if (relativeTo == null) {
                    throw new PolicyValidationException("files entitlement with a 'relative_path' must specify 'relative_to'");
                }
                final var baseDir = parseBaseDir(relativeTo);

                Path relativePath = Path.of(relativePathAsString);
                if (relativePath.isAbsolute()) {
                    throw new PolicyValidationException("'relative_path' must be relative");
                }
                filesData.add(FileData.ofRelativePath(relativePath, baseDir, parseMode(mode)));
            } else {
                Path path = Path.of(pathAsString);
                if (path.isAbsolute() == false) {
                    throw new PolicyValidationException("'path' must be absolute");
                }
                filesData.add(FileData.ofPath(path, parseMode(mode)));
            }
        }
        return new FilesEntitlement(filesData);
    }
}
