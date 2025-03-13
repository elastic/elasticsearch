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
import org.elasticsearch.entitlement.runtime.policy.FileUtils;
import org.elasticsearch.entitlement.runtime.policy.PathLookup;
import org.elasticsearch.entitlement.runtime.policy.Platform;
import org.elasticsearch.entitlement.runtime.policy.PolicyValidationException;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
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
        SHARED_REPO,
        HOME
    }

    public sealed interface FileData {

        Stream<Path> resolvePaths(PathLookup pathLookup);

        Mode mode();

        boolean exclusive();

        FileData withExclusive(boolean exclusive);

        Platform platform();

        FileData withPlatform(Platform platform);

        static FileData ofPath(Path path, Mode mode) {
            return new AbsolutePathFileData(path, mode, null, false);
        }

        static FileData ofRelativePath(Path relativePath, BaseDir baseDir, Mode mode) {
            return new RelativePathFileData(relativePath, baseDir, mode, null, false);
        }

        static FileData ofPathSetting(String setting, BaseDir baseDir, Mode mode) {
            return new PathSettingFileData(setting, baseDir, mode, null, false);
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

    private record AbsolutePathFileData(Path path, Mode mode, Platform platform, boolean exclusive) implements FileData {

        @Override
        public AbsolutePathFileData withExclusive(boolean exclusive) {
            return new AbsolutePathFileData(path, mode, platform, exclusive);
        }

        @Override
        public Stream<Path> resolvePaths(PathLookup pathLookup) {
            return Stream.of(path);
        }

        @Override
        public FileData withPlatform(Platform platform) {
            if (platform == platform()) {
                return this;
            }
            return new AbsolutePathFileData(path, mode, platform, exclusive);
        }
    }

    private record RelativePathFileData(Path relativePath, BaseDir baseDir, Mode mode, Platform platform, boolean exclusive)
        implements
            FileData,
            RelativeFileData {

        @Override
        public RelativePathFileData withExclusive(boolean exclusive) {
            return new RelativePathFileData(relativePath, baseDir, mode, platform, exclusive);
        }

        @Override
        public Stream<Path> resolveRelativePaths(PathLookup pathLookup) {
            return Stream.of(relativePath);
        }

        @Override
        public FileData withPlatform(Platform platform) {
            if (platform == platform()) {
                return this;
            }
            return new RelativePathFileData(relativePath, baseDir, mode, platform, exclusive);
        }
    }

    private record PathSettingFileData(String setting, BaseDir baseDir, Mode mode, Platform platform, boolean exclusive)
        implements
            RelativeFileData {

        @Override
        public PathSettingFileData withExclusive(boolean exclusive) {
            return new PathSettingFileData(setting, baseDir, mode, platform, exclusive);
        }

        @Override
        public Stream<Path> resolveRelativePaths(PathLookup pathLookup) {
            Stream<String> result = pathLookup.settingResolver()
                .apply(setting)
                .filter(s -> s.toLowerCase(Locale.ROOT).startsWith("https://") == false)
                .distinct();
            return result.map(Path::of);
        }

        @Override
        public FileData withPlatform(Platform platform) {
            if (platform == platform()) {
                return this;
            }
            return new PathSettingFileData(setting, baseDir, mode, platform, exclusive);
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

    private static Platform parsePlatform(String platform) {
        if (platform.equals("linux")) {
            return Platform.LINUX;
        } else if (platform.equals("macos")) {
            return Platform.MACOS;
        } else if (platform.equals("windows")) {
            return Platform.WINDOWS;
        } else {
            throw new PolicyValidationException("invalid platform: " + platform + ", valid values: [linux, macos, windows]");
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
        BiFunction<Map<String, Object>, String, String> checkString = (values, key) -> {
            Object value = values.remove(key);
            if (value == null) {
                return null;
            } else if (value instanceof String str) {
                return str;
            }
            throw new PolicyValidationException(
                "expected ["
                    + key
                    + "] to be type ["
                    + String.class.getSimpleName()
                    + "] but found type ["
                    + value.getClass().getSimpleName()
                    + "]"
            );
        };
        BiFunction<Map<String, Object>, String, Boolean> checkBoolean = (values, key) -> {
            Object value = values.remove(key);
            if (value == null) {
                return null;
            } else if (value instanceof Boolean bool) {
                return bool;
            }
            throw new PolicyValidationException(
                "expected ["
                    + key
                    + "] to be type ["
                    + boolean.class.getSimpleName()
                    + "] but found type ["
                    + value.getClass().getSimpleName()
                    + "]"
            );
        };
        List<FileData> filesData = new ArrayList<>();
        for (Object object : paths) {
            Map<String, Object> file = new HashMap<>((Map<String, Object>) object);
            String pathAsString = checkString.apply(file, "path");
            String relativePathAsString = checkString.apply(file, "relative_path");
            String relativeTo = checkString.apply(file, "relative_to");
            String pathSetting = checkString.apply(file, "path_setting");
            String settingBaseDirAsString = checkString.apply(file, "basedir_if_relative");
            String modeAsString = checkString.apply(file, "mode");
            String platformAsString = checkString.apply(file, "platform");
            Boolean exclusiveBoolean = checkBoolean.apply(file, "exclusive");
            boolean exclusive = exclusiveBoolean != null && exclusiveBoolean;

            if (file.isEmpty() == false) {
                throw new PolicyValidationException("unknown key(s) [" + file + "] in a listed file for files entitlement");
            }
            int foundKeys = (pathAsString != null ? 1 : 0) + (relativePathAsString != null ? 1 : 0) + (pathSetting != null ? 1 : 0);
            if (foundKeys != 1) {
                throw new PolicyValidationException(
                    "a files entitlement entry must contain one of " + "[path, relative_path, path_setting]"
                );
            }

            if (modeAsString == null) {
                throw new PolicyValidationException("files entitlement must contain 'mode' for every listed file");
            }
            Mode mode = parseMode(modeAsString);
            Platform platform = null;
            if (platformAsString != null) {
                platform = parsePlatform(platformAsString);
            }

            if (relativeTo != null && relativePathAsString == null) {
                throw new PolicyValidationException("'relative_to' may only be used with 'relative_path'");
            }

            if (settingBaseDirAsString != null && pathSetting == null) {
                throw new PolicyValidationException("'basedir_if_relative' may only be used with 'path_setting'");
            }

            final FileData fileData;
            if (relativePathAsString != null) {
                if (relativeTo == null) {
                    throw new PolicyValidationException("files entitlement with a 'relative_path' must specify 'relative_to'");
                }
                BaseDir baseDir = parseBaseDir(relativeTo);
                Path relativePath = Path.of(relativePathAsString);
                if (FileUtils.isAbsolutePath(relativePathAsString)) {
                    throw new PolicyValidationException("'relative_path' [" + relativePathAsString + "] must be relative");
                }
                fileData = FileData.ofRelativePath(relativePath, baseDir, mode);
            } else if (pathAsString != null) {
                Path path = Path.of(pathAsString);
                if (FileUtils.isAbsolutePath(pathAsString) == false) {
                    throw new PolicyValidationException("'path' [" + pathAsString + "] must be absolute");
                }
                fileData = FileData.ofPath(path, mode);
            } else if (pathSetting != null) {
                if (settingBaseDirAsString == null) {
                    throw new PolicyValidationException("files entitlement with a 'path_setting' must specify 'basedir_if_relative'");
                }
                BaseDir baseDir = parseBaseDir(settingBaseDirAsString);
                fileData = FileData.ofPathSetting(pathSetting, baseDir, mode);
            } else {
                throw new AssertionError("File entry validation error");
            }
            filesData.add(fileData.withPlatform(platform).withExclusive(exclusive));
        }
        return new FilesEntitlement(filesData);
    }
}
