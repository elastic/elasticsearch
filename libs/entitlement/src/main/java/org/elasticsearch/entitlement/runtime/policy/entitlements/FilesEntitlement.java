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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static java.lang.Character.isLetter;

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
        SHARED_REPO,
        HOME
    }

    public sealed interface FileData {

        Stream<Path> resolvePaths(PathLookup pathLookup);

        Mode mode();

        static FileData ofPath(Path path, Mode mode) {
            return new AbsolutePathFileData(path, mode);
        }

        static FileData ofRelativePath(Path relativePath, BaseDir baseDir, Mode mode) {
            return new RelativePathFileData(relativePath, baseDir, mode);
        }

        static FileData ofPathSetting(String setting, Mode mode) {
            return new PathSettingFileData(setting, mode);
        }

        static FileData ofRelativePathSetting(String setting, BaseDir baseDir, Mode mode) {
            return new RelativePathSettingFileData(setting, baseDir, mode);
        }

        /**
         * Tests if a path is absolute or relative, taking into consideration both Unix and Windows conventions.
         * Note that this leads to a conflict, resolved in favor of Unix rules: `/foo` can be either a Unix absolute path, or a Windows
         * relative path with "wrong" directory separator (using non-canonical slash in Windows).
         */
        static boolean isAbsolutePath(String path) {
            if (path.isEmpty()) {
                return false;
            }
            if (path.charAt(0) == '/') {
                // Unix/BSD absolute
                return true;
            }

            return isWindowsAbsolutePath(path);
        }

        private static boolean isSlash(char c) {
            return (c == '\\') || (c == '/');
        }

        private static boolean isWindowsAbsolutePath(String input) {
            // if a prefix is present, we expected (long) UNC or (long) absolute
            if (input.startsWith("\\\\?\\")) {
                return true;
            }

            if (input.length() > 1) {
                char c0 = input.charAt(0);
                char c1 = input.charAt(1);
                char c = 0;
                int next = 2;
                if (isSlash(c0) && isSlash(c1)) {
                    // Two slashes or more: UNC
                    return true;
                }
                if (isLetter(c0) && c1 == ':') {
                    // A drive: absolute
                    return true;
                }
            }
            // Otherwise relative
            return false;
        }
    }

    private sealed interface RelativeFileData extends FileData {
        BaseDir baseDir();

        Stream<Path> resolveRelativePaths(PathLookup pathLookup);

        @Override
        default Stream<Path> resolvePaths(PathLookup pathLookup) {
            Objects.requireNonNull(pathLookup);
            var relativePaths = resolveRelativePaths(pathLookup);
            switch (baseDir()) {
                case CONFIG:
                    return relativePaths.map(relativePath -> pathLookup.configDir().resolve(relativePath));
                case DATA:
                    return relativePathsCombination(pathLookup.dataDirs(), relativePaths);
                case SHARED_REPO:
                    return relativePathsCombination(pathLookup.sharedRepoDirs(), relativePaths);
                case HOME:
                    return relativePaths.map(relativePath -> pathLookup.homeDir().resolve(relativePath));
                default:
                    throw new IllegalArgumentException();
            }
        }
    }

    private static Stream<Path> relativePathsCombination(Path[] baseDirs, Stream<Path> relativePaths) {
        // multiple base dirs are a pain...we need the combination of the base dirs and relative paths
        List<Path> paths = new ArrayList<>();
        for (var relativePath : relativePaths.toList()) {
            for (var dataDir : baseDirs) {
                paths.add(dataDir.resolve(relativePath));
            }
        }
        return paths.stream();
    }

    private record AbsolutePathFileData(Path path, Mode mode) implements FileData {
        @Override
        public Stream<Path> resolvePaths(PathLookup pathLookup) {
            return Stream.of(path);
        }
    }

    private record RelativePathFileData(Path relativePath, BaseDir baseDir, Mode mode) implements FileData, RelativeFileData {
        @Override
        public Stream<Path> resolveRelativePaths(PathLookup pathLookup) {
            return Stream.of(relativePath);
        }
    }

    private record PathSettingFileData(String setting, Mode mode) implements FileData {
        @Override
        public Stream<Path> resolvePaths(PathLookup pathLookup) {
            return resolvePathSettings(pathLookup, setting);
        }
    }

    private record RelativePathSettingFileData(String setting, BaseDir baseDir, Mode mode) implements FileData, RelativeFileData {
        @Override
        public Stream<Path> resolveRelativePaths(PathLookup pathLookup) {
            return resolvePathSettings(pathLookup, setting);
        }
    }

    private static Stream<Path> resolvePathSettings(PathLookup pathLookup, String setting) {
        if (setting.contains("*")) {
            return pathLookup.settingGlobResolver().apply(setting).map(Path::of);
        }
        String path = pathLookup.settingResolver().apply(setting);
        return path == null ? Stream.of() : Stream.of(Path.of(path));
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
            // NOTE: shared_repo is _not_ accessible to policy files, only internally
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
            String pathSetting = file.remove("path_setting");
            String relativePathSetting = file.remove("relative_path_setting");
            String modeAsString = file.remove("mode");

            if (file.isEmpty() == false) {
                throw new PolicyValidationException("unknown key(s) [" + file + "] in a listed file for files entitlement");
            }
            int foundKeys = (pathAsString != null ? 1 : 0) + (relativePathAsString != null ? 1 : 0) + (pathSetting != null ? 1 : 0)
                + (relativePathSetting != null ? 1 : 0);
            if (foundKeys != 1) {
                throw new PolicyValidationException(
                    "a files entitlement entry must contain one of " + "[path, relative_path, path_setting, relative_path_setting]"
                );
            }

            if (modeAsString == null) {
                throw new PolicyValidationException("files entitlement must contain 'mode' for every listed file");
            }
            Mode mode = parseMode(modeAsString);

            BaseDir baseDir = null;
            if (relativeTo != null) {
                baseDir = parseBaseDir(relativeTo);
            }

            if (relativePathAsString != null) {
                if (baseDir == null) {
                    throw new PolicyValidationException("files entitlement with a 'relative_path' must specify 'relative_to'");
                }

                if (FileData.isAbsolutePath(relativePathAsString)) {
                    throw new PolicyValidationException("'relative_path' [" + relativePathAsString + "] must be relative");
                }
                filesData.add(FileData.ofRelativePath(Path.of(relativePathAsString), baseDir, mode));
            } else if (pathAsString != null) {
                if (FileData.isAbsolutePath(pathAsString) == false) {
                    throw new PolicyValidationException("'path' [" + pathAsString + "] must be absolute");
                }
                filesData.add(FileData.ofPath(Path.of(pathAsString), mode));
            } else if (pathSetting != null) {
                filesData.add(FileData.ofPathSetting(pathSetting, mode));
            } else if (relativePathSetting != null) {
                if (baseDir == null) {
                    throw new PolicyValidationException("files entitlement with a 'relative_path_setting' must specify 'relative_to'");
                }
                filesData.add(FileData.ofRelativePathSetting(relativePathSetting, baseDir, mode));
            } else {
                throw new AssertionError("File entry validation error");
            }
        }
        return new FilesEntitlement(filesData);
    }
}
