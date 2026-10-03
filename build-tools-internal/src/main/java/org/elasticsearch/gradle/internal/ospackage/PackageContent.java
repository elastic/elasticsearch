/*
 * @notice
 * Copyright 2011-2019 the original author or authors.
 * Modifications copyright (C) 2026 Elasticsearch B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This file is derived from the nebula gradle-ospackage-plugin
 * (https://github.com/nebula-plugins/gradle-ospackage-plugin), reduced to the
 * functionality used by the Elasticsearch build and rewritten in Java.
 */

package org.elasticsearch.gradle.internal.ospackage;

import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;

import java.io.Serializable;

/**
 * A declarative mapping of source files onto a destination inside the package, together with the
 * packaging metadata for the mapped entries. Directory sources are walked recursively (preserving
 * in-tree symbolic links as package link entries); plain file sources map to a single entry,
 * optionally renamed.
 */
public abstract class PackageContent {

    /** An ordered permission override for entries whose source-relative path matches an ant pattern. */
    public record PermissionRule(String pattern, int mode) implements Serializable {}

    /** The source files or directories; directories are walked recursively. */
    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract ConfigurableFileCollection getSource();

    /** The absolute destination path inside the package. */
    @Input
    public abstract Property<String> getInto();

    @Input
    @Optional
    public abstract ListProperty<String> getIncludes();

    @Input
    @Optional
    public abstract ListProperty<String> getExcludes();

    /** Default unix mode for mapped files; falls back to the source file mode when absent. */
    @Input
    @Optional
    public abstract Property<Integer> getFileMode();

    /** Default unix mode for mapped directories; falls back to the source directory mode when absent. */
    @Input
    @Optional
    public abstract Property<Integer> getDirMode();

    /** Owner of the mapped entries; falls back to the task-wide default. */
    @Input
    @Optional
    public abstract Property<String> getUser();

    /** Group of the mapped entries; falls back to the task-wide default. */
    @Input
    @Optional
    public abstract Property<String> getPermissionGroup();

    /** Whether mapped directories get the setgid bit. */
    @Input
    @Optional
    public abstract Property<Boolean> getSetgid();

    /** Whether walked directories become package-owned directory entries. */
    @Input
    @Optional
    public abstract Property<Boolean> getOwnDirectories();

    /**
     * When set, every ancestor directory of a mapped entry that is a strict descendant of this
     * path becomes a package-owned {@code 0755} directory entry, so the package manager removes
     * the intermediate directories on uninstall.
     */
    @Input
    @Optional
    public abstract Property<String> getOwnParentDirectories();

    /** RPM only: raw {@code Directive#RPMFILE_*} flag bits applied to the mapped files. */
    @Input
    @Optional
    public abstract Property<Integer> getFileType();

    /** Destination file name override for single-file sources. */
    @Input
    @Optional
    public abstract Property<String> getRename();

    /** Ordered permission overrides by source-relative ant pattern; first match wins. */
    @Input
    @Optional
    public abstract ListProperty<PermissionRule> getPermissionRules();

    // ------------------------------------------------------------------
    // DSL convenience methods
    // ------------------------------------------------------------------

    public void into(String destination) {
        getInto().set(destination);
    }

    public void include(String... patterns) {
        for (String pattern : patterns) {
            getIncludes().add(pattern);
        }
    }

    public void exclude(String... patterns) {
        for (String pattern : patterns) {
            getExcludes().add(pattern);
        }
    }

    public void fileMode(int mode) {
        getFileMode().set(mode);
    }

    public void dirMode(int mode) {
        getDirMode().set(mode);
    }

    public void user(String user) {
        getUser().set(user);
    }

    public void permissionGroup(String permissionGroup) {
        getPermissionGroup().set(permissionGroup);
    }

    public void setgid(boolean setgid) {
        getSetgid().set(setgid);
    }

    public void ownDirectories(boolean ownDirectories) {
        getOwnDirectories().set(ownDirectories);
    }

    public void ownParentDirectories(String belowPath) {
        getOwnParentDirectories().set(belowPath);
    }

    public void fileType(int fileTypeFlags) {
        getFileType().set(fileTypeFlags);
    }

    public void rename(String name) {
        getRename().set(name);
    }

    public void filePermissions(String pattern, int mode) {
        getPermissionRules().add(new PermissionRule(pattern, mode));
    }
}
