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

import groovy.lang.Closure;

import org.gradle.api.file.CopySpec;

import javax.inject.Inject;

/**
 * The project level {@code ospackage} extension. In addition to the shared package metadata of
 * {@link SystemPackagingExtension} it carries a copy spec that every packaging task of the project
 * inherits as if its content had been declared on the task itself.
 */
public abstract class ProjectPackagingExtension extends SystemPackagingExtension {

    private final CopySpec delegateCopySpec;

    @Inject
    public ProjectPackagingExtension(CopySpec delegateCopySpec) {
        this.delegateCopySpec = delegateCopySpec;
    }

    public CopySpec getDelegateCopySpec() {
        return delegateCopySpec;
    }

    public CopySpec into(Object destPath) {
        return delegateCopySpec.into(destPath);
    }

    public CopySpec into(Object destPath, Closure<?> closure) {
        return delegateCopySpec.into(destPath, child -> EnhancedCopySpec.configure(closure, child, null));
    }

    public CopySpec from(Object... sourcePaths) {
        return delegateCopySpec.from(sourcePaths);
    }

    public CopySpec from(Object sourcePath, Closure<?> closure) {
        return delegateCopySpec.from(sourcePath, child -> EnhancedCopySpec.configure(closure, child, null));
    }

    /** Default unix permissions for packaged files, e.g. {@code 0644}. */
    public void setFileMode(int mode) {
        delegateCopySpec.filePermissions(permissions -> permissions.unix(mode));
    }

    /** Default unix permissions for packaged directories, e.g. {@code 0755}. */
    public void setDirMode(int mode) {
        delegateCopySpec.dirPermissions(permissions -> permissions.unix(mode));
    }
}
