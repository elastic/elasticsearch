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
import org.elasticsearch.entitlement.runtime.policy.PathLookup;
import org.elasticsearch.entitlement.runtime.policy.PolicyValidationException;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

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
        DATA,
        HOME
    }

    public sealed interface FileData {

        Stream<Path> resolvePaths(PathLookup pathLookup);

        Mode mode();

        static FileData ofPath(Path path, Mode mode) {
            assert path.isAbsolute();
            return new AbsolutePathFileData(path, mode);
        }

        static FileData ofRelativePath(Path relativePath, BaseDir baseDir, Mode mode) {
            assert relativePath.isAbsolute() == false;
            return new RelativePathFileData(relativePath, baseDir, mode);
        }
    }

    private record AbsolutePathFileData(Path path, Mode mode) implements FileData {
        @Override
        public Stream<Path> resolvePaths(PathLookup pathLookup) {
            return Stream.of(path);
        }
    }

    private record RelativePathFileData(Path relativePath, BaseDir baseDir, Mode mode) implements FileData {

        @Override
        public Stream<Path> resolvePaths(PathLookup pathLookup) {
            Objects.requireNonNull(pathLookup);
            switch (baseDir) {
                case CONFIG:
                    return Stream.of(pathLookup.configDir().resolve(relativePath));
                case DATA:
                    return Arrays.stream(pathLookup.dataDirs()).map(d -> d.resolve(relativePath));
                case HOME:
                    return Stream.of(pathLookup.homeDir().resolve(relativePath));
                default:
                    throw new IllegalArgumentException();
            }
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
        return switch (baseDir) {
            case "config" -> BaseDir.CONFIG;
            case "data" -> BaseDir.DATA;
            case "home" -> BaseDir.HOME;
            default -> throw new PolicyValidationException(
                "invalid relative directory: " + baseDir + ", valid values: [config, data, home]"
            );
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
            if (pathAsString != null && relativePathAsString != null) {
                throw new PolicyValidationException("a files entitlement entry cannot contain both 'path' and 'relative_path'");
            }

            if (relativePathAsString != null) {
                if (relativeTo == null) {
                    throw new PolicyValidationException("files entitlement with a 'relative_path' must specify 'relative_to'");
                }
                final BaseDir baseDir = parseBaseDir(relativeTo);

                Path relativePath = Path.of(relativePathAsString);
                if (relativePath.isAbsolute()) {
                    throw new PolicyValidationException("'relative_path' [" + relativePathAsString + "] must be relative");
                }
                filesData.add(FileData.ofRelativePath(relativePath, baseDir, parseMode(mode)));
            } else if (pathAsString != null) {
                Path path = Path.of(pathAsString);
                if (path.isAbsolute() == false) {
                    throw new PolicyValidationException("'path' [" + pathAsString + "] must be absolute");
                }
                filesData.add(FileData.ofPath(path, parseMode(mode)));
            } else {
                throw new PolicyValidationException("files entitlement must contain either 'path' or 'relative_path' for every entry");
            }
        }
        return new FilesEntitlement(filesData);
    }
}
